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

const WORKER_UPGRADE_PREPARE = 'lit-channel:prepare-worker-upgrade'
const WORKER_UPGRADE_STATE = 'lit-channel:worker-upgrade-state'
const WORKER_UPGRADE_READY = 'lit-channel:worker-ready'
const WORKER_UPGRADE_COMPLETE = 'lit-channel:complete-worker-upgrade'
const DEFAULT_WORKER_UPGRADE_DEADLINE_MS = 5000
const SOCKET_REGISTRY_KEY = '__litChannelSharedSockets'

/**
 * @typedef {MessagePort | {
 * 	postMessage: Function,
 * 	onmessage: Function | null,
 * 	start: Function,
 * 	close?: Function,
 * 	addEventListener?: Function,
 * 	removeEventListener?: Function,
 * }} SharedSocketPort
 */

function getSocketRegistry() {
	if (typeof globalThis === 'undefined') return undefined
	if (!globalThis[SOCKET_REGISTRY_KEY]) {
		globalThis[SOCKET_REGISTRY_KEY] = new Set()
	}
	return globalThis[SOCKET_REGISTRY_KEY]
}

function createUpgradeId() {
	if (globalThis.crypto?.randomUUID) {
		return globalThis.crypto.randomUUID()
	}
	return `lit-channel-upgrade-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}

function normalizeDeadlineMs(value) {
	const deadlineMs = Number(value)
	if (!Number.isFinite(deadlineMs) || deadlineMs < 0) return DEFAULT_WORKER_UPGRADE_DEADLINE_MS
	return Math.floor(deadlineMs)
}

function controlMessageKey(type, upgradeId) {
	return `${type}:${upgradeId || ''}`
}

export async function reloadSharedWorkers(workerUrl, options = {}) {
	const registry = getSocketRegistry()
	if (!registry || registry.size === 0) return []

	const upgrades = []
	for (const socket of registry) {
		if (typeof socket?.upgradeWorker === 'function') {
			upgrades.push(socket.upgradeWorker(workerUrl, options))
		}
	}

	return Promise.all(upgrades)
}

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

export function getConfiguredAuthToken(doc = document) {
	if (!doc?.head) return undefined
	const authTokenMeta = doc.head.querySelector('meta[name="lit-channel-auth-token"]')
	const authToken = authTokenMeta?.getAttribute('content')?.trim()
	return authToken || undefined
}

function normalizeAuthToken(token) {
	if (typeof token !== 'string') return undefined
	const normalized = token.trim().replace(/^Bearer\s+/i, '')
	return normalized || undefined
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

	/** @type {string | undefined} */
	#authToken

	/** @type {any[]} */
	#queuedMessages = []

	/** @type {SharedSocketPort | null} */
	#port = null

	/** @type {SharedWorker | null} */
	#worker = null

	/** @type {'shared-worker' | 'broadcast-channel' | null} */
	#transportType = null

	/** @type {string | null} */
	#currentWorkerUrl = null

	/** @type {string | null} */
	#currentWorkerVersion = null

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

	/** @type {(event: MessageEvent) => void} */
	#portControlListener = (event) => {
		this.#handleControlMessage(event)
	}

	/** @type {EventTarget} */
	#eventTarget = new EventTarget()

	/** @type {number} */
	#reconnectAttempts = 0

	/** @type {number} */
	#maxReconnectAttempts = 10

	/** @type {AbortController | null} */
	#reconnectController = null

	/** @type {boolean} */
	#isUpgradingWorker = false

	/** @type {Promise<boolean> | null} */
	#workerUpgradePromise = null

	/** @type {Map<string, { resolve: Function, timeoutId: ReturnType<typeof setTimeout> | null }>} */
	#controlWaiters = new Map()

	/**
	 * @param {{ endpoint?: string, workerUrl?: string, authToken?: string }} [options]
	 */
	constructor(options = {}) {
		this.#endpoint = options.endpoint ?? getConfiguredEndpoint()
		this.#workerUrl = options.workerUrl ?? getConfiguredWorkerUrl()
		this.#authToken = normalizeAuthToken(options.authToken ?? getConfiguredAuthToken())
		getSocketRegistry()?.add(this)
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
				const { worker, port } = await this.#createSharedWorker(workerUrl)
				this.#worker = worker
				this.#port = port
				this.#transportType = 'shared-worker'
				this.#currentWorkerUrl = workerUrl
				this.#currentWorkerVersion = this.#readWorkerVersion(workerUrl)
				this.#postConfig(port, endpoint)

				// Set up message handler to detect port disconnection
				this.#setupPortMonitoring(port)

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
			this.#port = createBroadcastTransport({ endpoint, authToken: this.#authToken })
			this.#worker = null
			this.#transportType = 'broadcast-channel'
			this.#currentWorkerUrl = null
			this.#currentWorkerVersion = null
			this.#port.start()

			this.#setupPortMonitoring(this.#port)

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

	async #createSharedWorker(workerUrl) {
		const workerSource = await this.#resolveSharedWorkerSource(workerUrl)
		const worker = new SharedWorker(workerSource, {
			type: 'module',
			name: 'lit-channel',
		})
		const port = worker.port
		port.start()
		return { worker, port }
	}

	#readWorkerVersion(workerUrl) {
		try {
			const url = new URL(workerUrl, window.location.href)
			return url.searchParams.get('v') || url.searchParams.get('version') || url.toString()
		} catch (_) {
			return workerUrl || null
		}
	}

	#buildConfigMessage(endpoint, upgrade) {
		const configMessage = { type: 'config', endpoint }
		if (this.#authToken) {
			configMessage.authToken = this.#authToken
		}
		if (upgrade) {
			configMessage.upgrade = upgrade
		}
		return configMessage
	}

	#postConfig(port, endpoint, upgrade) {
		port.postMessage(this.#buildConfigMessage(endpoint, upgrade))
	}

	#setupPortMonitoring(port = this.#port) {
		if (!port) return

		port.onmessage = this.#messageHandler

		if (typeof port.removeEventListener === 'function') {
			port.removeEventListener('message', this.#portMessageListener)
			port.removeEventListener('message', this.#portControlListener)
		}
		if (typeof port.addEventListener === 'function') {
			port.addEventListener('message', this.#portMessageListener)
			port.addEventListener('message', this.#portControlListener)
		}
	}

	#teardownPortMonitoring(port) {
		if (!port) return
		if (typeof port.removeEventListener === 'function') {
			port.removeEventListener('message', this.#portMessageListener)
			port.removeEventListener('message', this.#portControlListener)
		}
		if (port.onmessage === this.#messageHandler) {
			port.onmessage = null
		}
	}

	#handleControlMessage(event) {
		const message = event?.data
		if (!message || typeof message !== 'object' || typeof message.type !== 'string') return false

		const key = controlMessageKey(message.type, message.upgradeId)
		const waiter = this.#controlWaiters.get(key)
		if (!waiter) return false

		this.#controlWaiters.delete(key)
		if (waiter.timeoutId !== null) {
			clearTimeout(waiter.timeoutId)
		}
		waiter.resolve(message)
		return true
	}

	#waitForControlMessage(type, upgradeId, deadlineMs) {
		const key = controlMessageKey(type, upgradeId)
		const existing = this.#controlWaiters.get(key)
		if (existing && existing.timeoutId !== null) {
			clearTimeout(existing.timeoutId)
		}

		return new Promise((resolve) => {
			const timeoutId = deadlineMs === 0
				? null
				: setTimeout(() => {
					this.#controlWaiters.delete(key)
					resolve(null)
				}, deadlineMs)

			this.#controlWaiters.set(key, { resolve, timeoutId })
		})
	}

	#safeClosePort(port) {
		if (!port?.close) return
		try {
			port.close()
		} catch (error) {
			console.debug('[SharedSocket] Error closing port:', error)
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
		if (this.#isUpgradingWorker) {
			this.#queuedMessages.push(data)
			console.debug('[SharedSocket] Queueing message (worker upgrade in progress)', data)
			return
		}

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

	/**
	 * Upgrade the active SharedWorker to a new script URL without reloading the page.
	 * @param {string} workerUrl
	 * @param {{ workerVersion?: string, deadlineMs?: number }} [options]
	 * @returns {Promise<boolean>} true when a SharedWorker transport was swapped
	 */
	async upgradeWorker(workerUrl, options = {}) {
		if (typeof window === 'undefined') return false
		if (!workerUrl || !('SharedWorker' in window)) return false

		this.#workerUrl = workerUrl
		if (!this.#port || this.#transportType !== 'shared-worker') return false
		if (this.#workerUpgradePromise) return this.#workerUpgradePromise

		this.#workerUpgradePromise = this.#doUpgradeWorker(workerUrl, options)
		try {
			return await this.#workerUpgradePromise
		} finally {
			this.#workerUpgradePromise = null
		}
	}

	async #doUpgradeWorker(workerUrl, options = {}) {
		const oldPort = this.#port
		const oldWorkerUrl = this.#currentWorkerUrl
		const oldWorkerVersion = this.#currentWorkerVersion
		const nextWorkerUrl = resolveWorkerUrl(workerUrl, window.location)
		const nextWorkerVersion = options.workerVersion || this.#readWorkerVersion(nextWorkerUrl)
		const deadlineMs = normalizeDeadlineMs(options.deadlineMs)
		const upgradeId = createUpgradeId()

		if (!oldPort) return false
		if (oldWorkerUrl === nextWorkerUrl) return false

		this.#isUpgradingWorker = true
		this.#emitEvent('upgrading', {
			upgradeId,
			from: oldWorkerUrl,
			to: nextWorkerUrl,
			workerVersion: nextWorkerVersion,
		})

		let newPort = null
		try {
			const statePromise = this.#waitForControlMessage(WORKER_UPGRADE_STATE, upgradeId, deadlineMs)
			oldPort.postMessage({
				type: WORKER_UPGRADE_PREPARE,
				upgradeId,
				nextWorkerUrl,
				nextWorkerVersion,
				deadlineMs,
			})

			const workerState = await statePromise
			const { worker, port } = await this.#createSharedWorker(nextWorkerUrl)
			newPort = port
			this.#setupPortMonitoring(newPort)

			const readyPromise = this.#waitForControlMessage(WORKER_UPGRADE_READY, upgradeId, deadlineMs)
			const endpoint = resolveWebSocketUrl(this.#endpoint, window.location)
			this.#postConfig(newPort, endpoint, {
				upgradeId,
				previousWorkerUrl: oldWorkerUrl,
				previousWorkerVersion: oldWorkerVersion,
				nextWorkerVersion,
				topics: workerState?.topics || [],
				pending: workerState?.pending || [],
			})

			const readyMessage = await readyPromise
			this.#teardownPortMonitoring(oldPort)
			this.#worker = worker
			this.#port = newPort
			this.#transportType = 'shared-worker'
			this.#currentWorkerUrl = nextWorkerUrl
			this.#currentWorkerVersion = readyMessage?.workerVersion || nextWorkerVersion
			this.#isUpgradingWorker = false

			this.#flushQueuedMessages()
			this.#emitEvent('reconnected', { upgradeId, workerVersion: this.#currentWorkerVersion })

			try {
				oldPort.postMessage({ type: WORKER_UPGRADE_COMPLETE, upgradeId })
			} catch (error) {
				console.debug('[SharedSocket] Failed to complete old worker upgrade:', error)
			}
			setTimeout(() => this.#safeClosePort(oldPort), 0)
			return true
		} catch (error) {
			if (newPort) {
				this.#teardownPortMonitoring(newPort)
				this.#safeClosePort(newPort)
			}
			if (oldWorkerUrl) {
				this.#workerUrl = oldWorkerUrl
			}
			this.#isUpgradingWorker = false
			this.#setupPortMonitoring(oldPort)
			this.#flushQueuedMessages()
			this.#emitEvent('upgrade-failed', { upgradeId, error })
			throw error
		}
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
		getSocketRegistry()?.delete(this)

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
		this.#teardownPortMonitoring(this.#port)
		this.#port = null
		this.#worker = null
		this.#transportType = null
		this.#currentWorkerUrl = null
		this.#currentWorkerVersion = null
		this.#isUpgradingWorker = false
		this.#workerUpgradePromise = null
		this.#queuedMessages = []
		this.#initPromise = null
		this.#clearInlineWorkerBlobUrl()
		for (const waiter of this.#controlWaiters.values()) {
			if (waiter.timeoutId !== null) clearTimeout(waiter.timeoutId)
			waiter.resolve(null)
		}
		this.#controlWaiters.clear()
	}
}
