/**
 * SharedSocket - Clean connection abstraction
 *
 * Provides a uniform, WebSocket-like interface for connecting to an implicitly
 * shared WebSocket. Automates the choice between SharedWorker (preferred) and
 * BroadcastChannel + leader election (fallback).
 *
 * Exposes: postMessage, onmessage, close(), and connection readiness checks.
 */

const sharedWorkerUrl = new URL('./shared-worker.js', import.meta.url)

export function getConfiguredEndpoint(doc = document) {
	if (!doc?.head) return undefined
	const endpointMeta = doc.head.querySelector('meta[name="lit-channel-endpoint"]')
	const endpoint = endpointMeta?.getAttribute('content')?.trim()
	return endpoint || undefined
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

export class SharedSocket {
	/** @type {string | undefined} */
	#endpoint

	/** @type {any[]} */
	#queuedMessages = []

	/** @type {MessagePort | { postMessage, onmessage, start, close } | null} */
	#port = null

	/** @type {Promise | null} */
	#initPromise = null

	/**
	 * @param {{ endpoint?: string }} [options]
	 */
	constructor(options = {}) {
		this.#endpoint = options.endpoint ?? getConfiguredEndpoint()
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
			try {
				const worker = new SharedWorker(new URL(sharedWorkerUrl), {
					type: 'module',
					name: 'lit-channel',
				})
				this.#port = worker.port
				this.#port.start()
				this.#port.postMessage({ type: 'config', endpoint })
				this.#flushQueuedMessages()
				console.debug('[SharedSocket] using SharedWorker transport')
				return
			} catch (error) {
				console.warn('[SharedSocket] SharedWorker failed, falling back', error)
			}
		}

		// 2. Fallback: BroadcastChannel + leader election
		try {
			const { createBroadcastTransport } = await import('./bc-transport.js')
			this.#port = createBroadcastTransport({ endpoint })
			this.#port.start()
			this.#flushQueuedMessages()
			console.debug('[SharedSocket] using BroadcastChannel transport (leader election)')
			return
		} catch (error) {
			console.warn('[SharedSocket] BroadcastChannel transport failed', error)
			throw error
		}
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
			return
		}

		this.#port.postMessage(data)
	}

	/**
	 * Set the message handler.
	 * @param {Function | null} handler
	 */
	set onmessage(handler) {
		if (this.#port) {
			this.#port.onmessage = handler
		}
	}

	/**
	 * Get the message handler.
	 */
	get onmessage() {
		return this.#port?.onmessage ?? null
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
		if (this.#port?.close) {
			await this.#port.close()
		}
		this.#port = null
		this.#queuedMessages = []
		this.#initPromise = null
	}
}
