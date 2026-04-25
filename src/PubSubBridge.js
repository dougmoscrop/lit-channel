/**
 * PubSubBridge - Pub/Sub abstraction over a WebSocket-like connection
 *
 * Provides subscribe, unsubscribe, and publish functionality by managing local
 * topic subscriptions and delegating message routing to the connection.
 *
 * Features:
 * - Automatic resubscription after connection failures
 * - Topic subscription persistence
 * - Handles ping/pong for connection health
 * - Optional resume/ack protocol support with eventId dedupe
 *
 * Takes any WebSocket-like object with postMessage/onmessage interface.
 */

export const DEFAULT_EVENT_ID_DEDUPE_LIMIT = 1024

function createSessionId() {
	if (globalThis.crypto?.randomUUID) {
		return globalThis.crypto.randomUUID()
	}
	return `lit-channel-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}

function normalizeStreamSeq(value) {
	const streamSeq = typeof value === 'number'
		? value
		: typeof value === 'string' && value.trim() !== ''
			? Number(value)
			: NaN

	if (!Number.isSafeInteger(streamSeq) || streamSeq < 0) return undefined
	return streamSeq
}

function normalizeEventId(value) {
	if (typeof value === 'string') {
		const eventId = value.trim()
		return eventId || undefined
	}
	if (typeof value === 'number' && Number.isFinite(value)) {
		return String(value)
	}
	return undefined
}

function normalizeDedupeLimit(value) {
	if (value === undefined) return DEFAULT_EVENT_ID_DEDUPE_LIMIT
	const limit = Number(value)
	if (!Number.isFinite(limit) || limit < 0) return DEFAULT_EVENT_ID_DEDUPE_LIMIT
	return Math.floor(limit)
}

export class PubSubBridge {
	/**
	 * @type {Object} - WebSocket-like object with postMessage/onmessage
	 */
	#connection

	/**
	 * @type {Map<string, Set<Function>>} - topic → Set<callback>
	 */
	#listeners = new Map()

	/**
	 * @type {Set<string>} - topics we're actively subscribed to (for recovery)
	 */
	#activeSubscriptions = new Set()

	/**
	 * @type {boolean} - Track if we've attached the message handler
	 */
	#handlerAttached = false

	/**
	 * @type {ReturnType<typeof setTimeout> | null} - Counter to debounce rapid resubscriptions
	 */
	#resubscribeTaskId = null

	/** @type {boolean} */
	#resumeEnabled = false

	/** @type {string | undefined} */
	#sessionId

	/** @type {((topic: string) => any) | undefined} */
	#getResumeCursor

	/** @type {number} */
	#eventIdDedupeLimit = DEFAULT_EVENT_ID_DEDUPE_LIMIT

	/** @type {Map<string, { streamSeq: number, cursor: string, sessionId: string }>} */
	#resumeCursors = new Map()

	/** @type {Map<string, { ids: Set<string>, order: string[] }>} */
	#seenEventIds = new Map()

	/**
	 * Create a new PubSubBridge.
	 * @param {Object} connection - WebSocket-like object with postMessage and onmessage support
	 * @param {{ resumeEnabled?: boolean, sessionId?: string, getResumeCursor?: (topic: string) => any, eventIdDedupeLimit?: number }} [options]
	 */
	constructor(connection, options = {}) {
		if (!connection || typeof connection.postMessage !== 'function') {
			throw new Error('connection must have a postMessage method')
		}
		this.#connection = connection
		this.#resumeEnabled = options.resumeEnabled === true
		this.#sessionId = this.#resumeEnabled
			? String(options.sessionId || createSessionId())
			: undefined
		this.#getResumeCursor = typeof options.getResumeCursor === 'function'
			? options.getResumeCursor
			: undefined
		this.#eventIdDedupeLimit = normalizeDedupeLimit(options.eventIdDedupeLimit)
		this.#attachConnectionListener()
		this.#monitorConnectionEvents()
	}

	get resumeEnabled() {
		return this.#resumeEnabled
	}

	get sessionId() {
		return this.#sessionId
	}

	/**
	 * Wire up the onmessage handler on the connection.
	 */
	#attachConnectionListener() {
		if (this.#handlerAttached) return

		const originalHandler = this.#connection.onmessage
		this.#connection.onmessage = (e) => {
			// Call original handler if it exists
			originalHandler?.call(this.#connection, e)

			// Route messages by topic to subscribers
			const { type, topic, payload } = e.data

			if (type === 'ping') {
				console.debug('[PubSubBridge] received ping, sending pong')
				this.#connection.postMessage({ type: 'pong' })
				return
			}

			if (type === 'pong') {
				console.debug('[PubSubBridge] received pong')
				return
			}

			if (type === 'message' && topic) {
				if (!this.#processResumeEnvelope(topic, payload)) return

				const cbs = this.#listeners.get(topic)
				if (cbs) {
					for (const cb of cbs) {
						try {
							cb(payload, topic)
						} catch (err) {
							console.error('[PubSubBridge] Error in callback for topic', topic, err)
						}
					}
				}
			}
		}

		this.#handlerAttached = true
	}

	#processResumeEnvelope(topic, payload) {
		if (!this.#resumeEnabled) return true

		const metadata = payload && typeof payload === 'object'
			? payload.__rt
			: undefined
		if (!metadata || typeof metadata !== 'object') return true

		this.#ackStreamSeq(topic, metadata.streamSeq)

		if (Object.prototype.hasOwnProperty.call(metadata, 'eventId')) {
			return this.#rememberEventId(topic, metadata.eventId)
		}

		return true
	}

	#ackStreamSeq(topic, value) {
		const streamSeq = normalizeStreamSeq(value)
		if (streamSeq === undefined || !this.#sessionId) return

		const didAdvance = this.#storeResumeCursor(topic, {
			streamSeq,
			cursor: String(streamSeq),
			sessionId: this.#sessionId,
		})
		if (!didAdvance) return

		this.#connection.postMessage({
			type: 'ack',
			topic,
			streamSeq,
			cursor: String(streamSeq),
			sessionId: this.#sessionId,
		})
	}

	#rememberEventId(topic, value) {
		if (this.#eventIdDedupeLimit === 0) return true

		const eventId = normalizeEventId(value)
		if (eventId === undefined) return true

		let seen = this.#seenEventIds.get(topic)
		if (!seen) {
			seen = { ids: new Set(), order: [] }
			this.#seenEventIds.set(topic, seen)
		}

		if (seen.ids.has(eventId)) return false

		seen.ids.add(eventId)
		seen.order.push(eventId)

		while (seen.order.length > this.#eventIdDedupeLimit) {
			const evicted = seen.order.shift()
			if (evicted !== undefined) seen.ids.delete(evicted)
		}

		return true
	}

	#normalizeResumeCursor(value) {
		if (!this.#sessionId || value == null) return undefined

		if (typeof value === 'number' || typeof value === 'string') {
			const streamSeq = normalizeStreamSeq(value)
			if (streamSeq === undefined) return undefined
			return { streamSeq, cursor: String(streamSeq), sessionId: this.#sessionId }
		}

		if (typeof value !== 'object') return undefined

		const streamSeq = normalizeStreamSeq(value.streamSeq)
		if (streamSeq === undefined) return undefined

		const cursor = value.cursor === undefined || value.cursor === null
			? String(streamSeq)
			: String(value.cursor)

		return { streamSeq, cursor, sessionId: this.#sessionId }
	}

	#storeResumeCursor(topic, cursor) {
		const normalized = this.#normalizeResumeCursor(cursor)
		if (!normalized) return false

		const current = this.#resumeCursors.get(topic)
		if (current && normalized.streamSeq <= current.streamSeq) {
			return false
		}

		this.#resumeCursors.set(topic, normalized)
		return true
	}

	#readSeedCursor(topic) {
		if (!this.#getResumeCursor) return

		try {
			const seed = this.#getResumeCursor(topic)
			this.#storeResumeCursor(topic, seed)
		} catch (err) {
			console.warn('[PubSubBridge] Failed to read resume cursor for topic', topic, err)
		}
	}

	#buildSubscribeMessage(topic) {
		const message = { type: 'subscribe', topic }
		if (!this.#resumeEnabled) return message

		this.#readSeedCursor(topic)
		const resume = this.#resumeCursors.get(topic)
		if (resume) {
			message.resume = { ...resume }
		}

		return message
	}

	/**
	 * Monitor connection events for reconnection recovery.
	 */
	#monitorConnectionEvents() {
		if (this.#connection.addEventListener && typeof this.#connection.addEventListener === 'function') {
			this.#connection.addEventListener('reconnecting', (e) => {
				console.log('[PubSubBridge] Connection reconnecting (attempt %d)', e.detail?.attempt ?? 1)
			})

			this.#connection.addEventListener('reconnected', () => {
				console.log('[PubSubBridge] Connection restored, resubscribing to %d topics', this.#activeSubscriptions.size)
				this.#scheduleResubscribe()
			})

			this.#connection.addEventListener('reconnect-failed', (e) => {
				console.error('[PubSubBridge] Connection reconnection failed after %d attempts', e.detail?.maxAttempts ?? '?')
			})
		}
	}

	/**
	 * Debounced resubscribe - waits for multiple reconnect events to settle.
	 */
	#scheduleResubscribe() {
		if (this.#resubscribeTaskId !== null) {
			clearTimeout(this.#resubscribeTaskId)
		}

		this.#resubscribeTaskId = setTimeout(() => {
			this.#resubscribeTaskId = null
			this.#resubscribeAll()
		}, 100)  // Small delay to batch resubscriptions
	}

	/**
	 * Resubscribe to all active topics.
	 */
	#resubscribeAll() {
		if (this.#activeSubscriptions.size === 0) {
			console.debug('[PubSubBridge] No active subscriptions to recover')
			return
		}

		console.log('[PubSubBridge] Resubscribing to %d topics', this.#activeSubscriptions.size)
		const toResubscribe = Array.from(this.#activeSubscriptions)
		for (const topic of toResubscribe) {
			try {
				this.#connection.postMessage(this.#buildSubscribeMessage(topic))
				console.debug('[PubSubBridge] Resubscribed to', topic)
			} catch (err) {
				console.error('[PubSubBridge] Failed to resubscribe to topic', topic, err)
			}
		}
	}

	/**
	 * Subscribe to a topic.
	 * @param {string} topic
	 * @param {Function} callback - Receives (payload, topic)
	 * @returns {Function} - Unsubscribe function
	 */
	subscribe(topic, callback) {
		if (!this.#listeners.has(topic)) {
			this.#listeners.set(topic, new Set())
		}
		this.#listeners.get(topic).add(callback)

		// Only send subscribe if this is the first listener for this topic
		if (!this.#activeSubscriptions.has(topic)) {
			this.#activeSubscriptions.add(topic)
			try {
				this.#connection.postMessage(this.#buildSubscribeMessage(topic))
				console.debug('[PubSubBridge] Subscribed to', topic)
			} catch (err) {
				console.error('[PubSubBridge] Failed to subscribe to topic', topic, err)
				this.#activeSubscriptions.delete(topic)
				throw err
			}
		}

		return () => {
			this.unsubscribe(topic, callback)
		}
	}

	/**
	 * Unsubscribe a callback from a topic.
	 * @param {string} topic
	 * @param {Function} callback
	 */
	unsubscribe(topic, callback) {
		const cbs = this.#listeners.get(topic)
		if (!cbs) return

		cbs.delete(callback)
		if (cbs.size === 0) {
			this.#listeners.delete(topic)
			this.#activeSubscriptions.delete(topic)
			try {
				this.#connection.postMessage({ type: 'unsubscribe', topic })
				console.debug('[PubSubBridge] Unsubscribed from', topic)
			} catch (err) {
				console.error('[PubSubBridge] Failed to unsubscribe from topic', topic, err)
			}
		}
	}

	/**
	 * Publish a message to a topic.
	 * @param {string} topic
	 * @param {any} payload
	 */
	publish(topic, payload) {
		try {
			this.#connection.postMessage({ type: 'publish', topic, payload })
		} catch (err) {
			console.error('[PubSubBridge] Failed to publish to topic', topic, err)
			throw err
		}
	}
}
