/**
 * PubSubBridge - Pub/Sub abstraction over a WebSocket-like connection
 *
 * Provides subscribe, unsubscribe, and publish functionality by managing local
 * topic subscriptions and delegating message routing to the connection.
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
	 * @type {boolean} - Track if we've attached the message handler
	 */
	#handlerAttached = false

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
				this.#connection.postMessage({ type: 'pong' })
				return
			}

			if (type === 'message' && topic) {
				const cbs = this.#listeners.get(topic)
				if (cbs) {
					for (const cb of cbs) {
						cb(payload, topic)
					}
				}
			}
		}

		this.#handlerAttached = true
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

		this.#connection.postMessage({ type: 'subscribe', topic })

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
			this.#connection.postMessage({ type: 'unsubscribe', topic })
		}
	}

	/**
	 * Publish a message to a topic.
	 * @param {string} topic
	 * @param {any} payload
	 */
	publish(topic, payload) {
		this.#connection.postMessage({ type: 'publish', topic, payload })
	}
}
