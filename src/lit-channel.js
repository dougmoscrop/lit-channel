import { LitElement, html } from 'lit'
import { SharedSocket } from './SharedSocket.js'
import { PubSubBridge } from './PubSubBridge.js'

/**
 * @typedef {Object} LitChannelConfig
 * @property {string} [endpoint]
 * @property {string} [workerUrl]
 * @property {string} [authToken]
 * @property {boolean} [resumeEnabled]
 * @property {string} [sessionId]
 * @property {(topic: string) => any} [getResumeCursor]
 * @property {number} [eventIdDedupeLimit]
 */

// Singleton instances shared across all lit-channel elements
let sharedSocket = null
let pubSubBridge = null
let connectionPromise = null
/** @type {LitChannelConfig} */
let litChannelConfig = {}

export { SharedSocket, reloadSharedWorkers } from './SharedSocket.js'
export { PubSubBridge } from './PubSubBridge.js'

/**
 * Configure the singleton used by `<lit-channel>` elements.
 * Call before any element connects.
 * @param {LitChannelConfig} [config]
 */
export function configureLitChannel(config = {}) {
	if (sharedSocket || connectionPromise) {
		throw new Error('configureLitChannel must be called before lit-channel connects')
	}
	litChannelConfig = { ...litChannelConfig, ...config }
}

/**
 * @param {LitChannelConfig} [config]
 */
async function getConnection(config = {}) {
	if (connectionPromise) return connectionPromise
	const effectiveConfig = { ...litChannelConfig, ...config }

	connectionPromise = (async () => {
		if (!sharedSocket) {
			sharedSocket = new SharedSocket({
				endpoint: effectiveConfig.endpoint,
				workerUrl: effectiveConfig.workerUrl,
				authToken: effectiveConfig.authToken,
			})
			await sharedSocket.connect()
			pubSubBridge = new PubSubBridge(sharedSocket, effectiveConfig)
		}
		return pubSubBridge
	})()

	return connectionPromise
}

export async function __resetConnectionForTests() {
	await sharedSocket?.close?.()
	sharedSocket = null
	pubSubBridge = null
	connectionPromise = null
	litChannelConfig = {}
}

/**
 * Pure pub/sub channel component - invisible connector to shared WebSocket.
 * Subscribes to a topic and dispatches events when messages are received.
 * Use lit-channel-debug for a visual debugging interface.
 */
export class LitChannel extends LitElement {
	static properties = {
		name: { type: String, reflect: true },
		description: { type: String },
	}

	_unsubscribe = null
	_bridge = null
	_subscriptionAbortController = null
	_controlHandlers = []
	_lastSubscribedAck = null

	constructor() {
		super()
		this.name = 'Channel'
		this.description = ''
	}

	connectedCallback() {
		super.connectedCallback()
		getConnection().then((bridge) => {
			if (!this.isConnected) return

			this._bridge = bridge
			this._lastSubscribedAck = null
			this._unsubscribe = bridge.subscribe(this.name, (payload, topic) => {
				this.dispatchEvent(new CustomEvent('lit-channel-message', {
					detail: { topic, payload },
					bubbles: true,
					composed: true,
				}))
			})

			this._subscriptionAbortController = new AbortController()
			this._attachControlListeners(bridge)
			bridge.waitForSubscribed?.(this.name, { signal: this._subscriptionAbortController.signal })
				.then((ack) => this._dispatchSubscribed(ack))
				.catch((error) => {
					if (this._subscriptionAbortController?.signal.aborted) return
					if (error?.frame) return
					this._dispatchError(error?.frame || { type: 'error', topic: this.name, error })
				})
		})
	}

	disconnectedCallback() {
		super.disconnectedCallback()
		this._subscriptionAbortController?.abort()
		this._subscriptionAbortController = null
		this._detachControlListeners()
		this._unsubscribe?.()
		this._unsubscribe = null
		this._lastSubscribedAck = null
	}

	_attachControlListeners(bridge) {
		this._detachControlListeners()

		for (const type of ['subscribed', 'error', 'replay-gap', 'replay-complete', 'control']) {
			const listener = (event) => this._handleBridgeControl(type, event.detail)
			bridge.addEventListener(type, listener)
			this._controlHandlers.push({ type, listener })
		}
	}

	_detachControlListeners() {
		if (!this._bridge) {
			this._controlHandlers = []
			return
		}

		for (const { type, listener } of this._controlHandlers) {
			this._bridge.removeEventListener(type, listener)
		}
		this._controlHandlers = []
	}

	_handleBridgeControl(type, detail) {
		if (!detail) return
		if (detail.topic && detail.topic !== this.name) return

		if (type === 'control') {
			this._dispatchControl(detail)
			return
		}

		if (type === 'subscribed') {
			this._dispatchSubscribed(detail)
			return
		}

		if (type === 'error') {
			this._dispatchError(detail)
			return
		}

		this._dispatchTypedControl(type, detail)
	}

	_dispatchSubscribed(ack) {
		if (!this.isConnected || !ack || ack.topic !== this.name) return
		if (ack === this._lastSubscribedAck) return
		this._lastSubscribedAck = ack
		this.dispatchEvent(new CustomEvent('lit-channel-subscribed', {
			detail: { topic: ack.topic, resume: ack.resume },
			bubbles: true,
			composed: true,
		}))
	}

	_dispatchError(frame) {
		if (!this.isConnected) return
		this.dispatchEvent(new CustomEvent('lit-channel-error', {
			detail: { frame },
			bubbles: true,
			composed: true,
		}))
	}

	_dispatchControl(frame) {
		if (!this.isConnected) return
		this.dispatchEvent(new CustomEvent('lit-channel-control', {
			detail: { frame },
			bubbles: true,
			composed: true,
		}))
	}

	_dispatchTypedControl(type, frame) {
		if (!this.isConnected) return
		this.dispatchEvent(new CustomEvent(`lit-channel-${type}`, {
			detail: { frame },
			bubbles: true,
			composed: true,
		}))
	}

	/**
	 * Publish a message to this channel's topic.
	 * @param {any} payload - The message payload to publish
	 */
	publish(payload) {
		if (!this._bridge) {
			console.warn('[lit-channel] Cannot publish - bridge not ready')
			return
		}
		this._bridge.publish(this.name, payload)
		this.dispatchEvent(new CustomEvent('lit-channel-send', {
			detail: { topic: this.name, payload },
			bubbles: true,
			composed: true,
		}))
	}

	render() {
		// Invisible component - just renders slotted content
		return html`<slot></slot>`
	}
}

customElements.define('lit-channel', LitChannel)
