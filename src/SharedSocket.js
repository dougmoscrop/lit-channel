/**
 * SharedSocket - Clean connection abstraction
 *
 * Provides a uniform, WebSocket-like interface for connecting to an implicitly
 * shared WebSocket. Automates the choice between SharedWorker (preferred) and
 * BroadcastChannel + leader election (fallback).
 *
 * Exposes: postMessage, onmessage, close(), and connection readiness checks.
 *
 * Features:
 * - Automatic reconnection with exponential backoff
 * - Connection ready/ready-for-reconnect events
 * - Subscription recovery on reconnect
 */

const sharedWorkerUrl = new URL('./shared-worker.js', import.meta.url)

export function getConfiguredEndpoint(doc = document) {
	if (!doc?.head) return undefined
	const endpointMeta = doc.head.querySelector('meta[name="lit-channel-endpoint"]')
	const endpoint = endpointMeta?.getAttribute('content')?.trim()
	return endpoint || undefined
}

export function getConfiguredWorkerUrl(doc = document) {
	if (!doc?.head) return undefined
	const workerMeta = doc.head.querySelector('meta[name="lit-channel-worker-url"]')
	const workerUrl = workerMeta?.getAttribute('content')?.trim()
	return workerUrl || undefined
}

export function resolveWebSocketUrl(endpoint, locationLike = location) {
	const defaultUrl = `${locationLike.protocol === 'https:' ? 'wss:' : 'ws:'}//${locationLike.host}/api/ws`
	if (!endpoint) return defaultUrl

	const resolved = new URL(endpoint, locationLike.href)
	if (resolved.protocol === 'ws:' || resolved.protocol === 'wss:') {
		return resolved.toString()
	}
	if (resolved.protocol === 'http:' || resolved.protocol === 'https:') {
		resolved.protocol = resolved.protocol === 'https:' ? 'wss:' : 'ws:'
		return resolved.toString()
	}

	throw new Error(`Invalid lit-channel endpoint protocol: ${resolved.protocol}`)
}

export function resolveWorkerUrl(workerUrl, locationLike = location, defaultUrl = sharedWorkerUrl) {
	if (!workerUrl) return defaultUrl.toString()

	const resolved = new URL(workerUrl, locationLike.href)
	if (resolved.protocol === 'http:' || resolved.protocol === 'https:') {
		return resolved.toString()
	}

	throw new Error(`Invalid lit-channel worker URL protocol: ${resolved.protocol}`)
}

export class SharedSocket {
	/** @type {string | undefined} */
	#endpoint

	/** @type {string | undefined} */
	#workerUrl

	/** @type {any[]} */
	#queuedMessages = []

	/** @type {MessagePort | { postMessage, onmessage, start, close } | null} */
	#port = null

	/** @type {Promise | null} */
	#initPromise = null

	/** @type {Function | null} */
	#messageHandler = null

	/** @type {string | null} */
	#inlineWorkerBlobUrl = null

	/** @type {(event: MessageEvent) => void} */
	#portMessageListener = () => {
		this.#reconnectAttempts = 0
	}

	/** @type {EventTarget} */
	#eventTarget = new EventTarget()

	/** @type {number} */
	#reconnectAttempts = 0

	/** @type {number} */
	#maxReconnectAttempts = 10

	/** @type {AbortController | null} */
	#reconnectController = null

	/**
	 * @param {{ endpoint?: string, workerUrl?: string }} [options]
	 */
	constructor(options = {}) {
		this.#endpoint = options.endpoint ?? getConfiguredEndpoint()
		this.#workerUrl = options.workerUrl ?? getConfiguredWorkerUrl()
	}

	/**
	 * Initialize connection (call once per page).
	 * Tries SharedWorker first, falls back to BroadcastChannel + leader election.
	 * @returns {Promise<void>}
	 */
	async connect() {
		if (typeof window === 'undefined') return
		if (this.#port) return
		if (this.#initPromise) return this.#initPromise

		this.#initPromise = this.#doInit()
		return this.#initPromise
	}

	async #doInit() {
		const endpoint = resolveWebSocketUrl(this.#endpoint, window.location)

		// 1. Try SharedWorker
		if ('SharedWorker' in window) {
			const workerUrl = resolveWorkerUrl(this.#workerUrl, window.location)

			try {
				const workerSource = await this.#resolveSharedWorkerSource(workerUrl)
				const worker = new SharedWorker(workerSource, {
					type: 'module',
					name: 'lit-channel',
				})
				this.#port = worker.port
				this.#port.start()
				this.#port.postMessage({ type: 'config', endpoint })

				// Set up message handler to detect port disconnection
				this.#setupPortMonitoring()

				this.#flushQueuedMessages()
				console.debug('[SharedSocket] using SharedWorker transport')
				this.#emitEvent('ready')
				return
			} catch (error) {
				this.#clearInlineWorkerBlobUrl()
				console.warn('[SharedSocket] SharedWorker failed, falling back', error)
			}
		}

		// 2. Fallback: BroadcastChannel + leader election
		try {
			const { createBroadcastTransport } = await import('./bc-transport.js')
			this.#port = createBroadcastTransport({ endpoint })
			this.#port.start()

			this.#setupPortMonitoring()

			this.#flushQueuedMessages()
			console.debug('[SharedSocket] using BroadcastChannel transport (leader election)')
			this.#emitEvent('ready')
			return
		} catch (error) {
			console.warn('[SharedSocket] BroadcastChannel transport failed', error)
			throw error
		}
	}

	#clearInlineWorkerBlobUrl() {
		if (!this.#inlineWorkerBlobUrl) return
		if (typeof URL.revokeObjectURL === 'function') {
			URL.revokeObjectURL(this.#inlineWorkerBlobUrl)
		}
		this.#inlineWorkerBlobUrl = null
	}

	#shouldUseDataWorkerUrl() {
		const userAgent = window.navigator?.userAgent?.toLowerCase() ?? ''
		return userAgent.includes('firefox')
	}

	async #resolveSharedWorkerSource(workerUrl) {
		const resolvedWorkerUrl = new URL(workerUrl, window.location.href)
		if (resolvedWorkerUrl.origin === window.location.origin) {
			return resolvedWorkerUrl.toString()
		}

		const response = await fetch(resolvedWorkerUrl.toString())
		if (!response.ok) {
			throw new Error(`Worker fetch failed (${response.status})`)
		}

		const source = await response.text()
		if (this.#shouldUseDataWorkerUrl()) {
			return `data:text/javascript;charset=utf-8,${encodeURIComponent(source)}`
		}

		if (typeof URL.createObjectURL === 'function') {
			this.#clearInlineWorkerBlobUrl()
			this.#inlineWorkerBlobUrl = URL.createObjectURL(new Blob([source], { type: 'text/javascript' }))
			return this.#inlineWorkerBlobUrl
		}

		return `data:text/javascript;charset=utf-8,${encodeURIComponent(source)}`
	}

	#setupPortMonitoring() {
		if (!this.#port) return

		this.#port.onmessage = this.#messageHandler

		if (typeof this.#port.removeEventListener === 'function') {
			this.#port.removeEventListener('message', this.#portMessageListener)
		}
		if (typeof this.#port.addEventListener === 'function') {
			this.#port.addEventListener('message', this.#portMessageListener)
		}
	}

	#emitEvent(eventType, detail) {
		const event = new CustomEvent(eventType, { detail })
		this.#eventTarget.dispatchEvent(event)
	}

	/**
	 * Add event listener for connection events.
	 * Events: 'ready' (connected), 'reconnecting', 'reconnect-failed'
	 */
	addEventListener(type, listener) {
		this.#eventTarget.addEventListener(type, listener)
	}

	removeEventListener(type, listener) {
		this.#eventTarget.removeEventListener(type, listener)
	}

	#flushQueuedMessages() {
		if (!this.#port || this.#queuedMessages.length === 0) return

		const queued = this.#queuedMessages.splice(0, this.#queuedMessages.length)
		for (const message of queued) {
			this.#port.postMessage(message)
		}
	}

	/**
	 * Send a message through the connection.
	 * @param {any} data
	 */
	postMessage(data) {
		if (!this.#port) {
			this.#queuedMessages.push(data)
			console.debug('[SharedSocket] Queueing message (not connected yet)', data)
			// Attempt reconnect if we know we should be connected
			if (this.#initPromise) {
				this.#attemptReconnect()
			}
			return
		}

		this.#port.postMessage(data)
	}

	async #attemptReconnect() {
		if (this.#reconnectController) {
			return  // Already attempting
		}

		this.#reconnectController = new AbortController()
		const signal = this.#reconnectController.signal

		try {
			while (this.#reconnectAttempts < this.#maxReconnectAttempts && !signal.aborted) {
				const backoffMs = Math.min(100 * Math.pow(2, this.#reconnectAttempts), 30000)
				console.warn(`[SharedSocket] Reconnection attempt ${this.#reconnectAttempts + 1}/${this.#maxReconnectAttempts}, waiting ${backoffMs}ms`)
				this.#emitEvent('reconnecting', { attempt: this.#reconnectAttempts + 1 })

				await new Promise(resolve => setTimeout(resolve, backoffMs))

				if (signal.aborted) break

				try {
					// Close existing port if any
					if (this.#port?.close) {
						try {
							await this.#port.close()
						} catch (e) {
							console.debug('[SharedSocket] Error closing old port:', e)
						}
					}

					this.#port = null
					this.#initPromise = null

					await this.connect()

					if (this.#port) {
						console.log('[SharedSocket] Reconnection successful')
						this.#reconnectAttempts = 0
						this.#reconnectController = null
						this.#flushQueuedMessages()
						this.#emitEvent('reconnected')
						return
					}
				} catch (error) {
					console.warn('[SharedSocket] Reconnection attempt failed:', error)
					this.#reconnectAttempts++
				}
			}

			// Max attempts exceeded
			console.error('[SharedSocket] Max reconnection attempts exceeded')
			this.#emitEvent('reconnect-failed', { maxAttempts: this.#maxReconnectAttempts })
		} finally {
			this.#reconnectController = null
		}
	}

	/**
	 * Set the message handler.
	 * @param {Function | null} handler
	 */
	set onmessage(handler) {
		this.#messageHandler = handler
		if (this.#port) this.#setupPortMonitoring()
	}

	/**
	 * Get the message handler.
	 */
	get onmessage() {
		return this.#messageHandler
	}

	/**
	 * Check if connection is ready.
	 */
	get isReady() {
		return this.#port !== null
	}

	/**
	 * Close the connection.
	 */
	async close() {
		// Cancel any pending reconnects
		if (this.#reconnectController) {
			this.#reconnectController.abort()
			this.#reconnectController = null
		}

		if (this.#port?.close) {
			try {
				await this.#port.close()
			} catch (e) {
				console.debug('[SharedSocket] Error closing port:', e)
			}
		}
		this.#port = null
		this.#queuedMessages = []
		this.#initPromise = null
		this.#clearInlineWorkerBlobUrl()
	}
}
