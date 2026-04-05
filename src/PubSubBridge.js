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
 *
 * Takes any WebSocket-like object with postMessage/onmessage interface.
 */

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
	 * @type {number} - Counter to debounce rapid resubscriptions
	 */
	#resubscribeTaskId = null

	/**
	 * Create a new PubSubBridge.
	 * @param {Object} connection - WebSocket-like object with postMessage and onmessage support
	 */
	constructor(connection) {
		if (!connection || typeof connection.postMessage !== 'function') {
			throw new Error('connection must have a postMessage method')
		}
		this.#connection = connection
		this.#attachConnectionListener()
		this.#monitorConnectionEvents()
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
				this.#connection.postMessage({ type: 'subscribe', topic })
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
				this.#connection.postMessage({ type: 'subscribe', topic })
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
