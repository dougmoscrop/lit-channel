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

export { SharedSocket } from './SharedSocket.js'
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

	constructor() {
		super()
		this.name = 'Channel'
		this.description = ''
	}

	connectedCallback() {
		super.connectedCallback()
		getConnection().then((bridge) => {
			this._bridge = bridge
			this._unsubscribe = bridge.subscribe(this.name, (payload, topic) => {
				this.dispatchEvent(new CustomEvent('lit-channel-message', {
					detail: { topic, payload },
					bubbles: true,
					composed: true,
				}))
			})
		})
	}

	disconnectedCallback() {
		super.disconnectedCallback()
		this._unsubscribe?.()
		this._unsubscribe = null
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
