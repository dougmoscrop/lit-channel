import { LitElement, html } from 'lit'
import { SharedSocket } from './SharedSocket.js'
import { PubSubBridge } from './PubSubBridge.js'

// Singleton instances shared across all lit-channel elements
let sharedSocket = null
let pubSubBridge = null
let connectionPromise = null

async function getConnection(config = {}) {
	if (connectionPromise) return connectionPromise

	connectionPromise = (async () => {
		if (!sharedSocket) {
			sharedSocket = new SharedSocket(config)
			await sharedSocket.connect()
			pubSubBridge = new PubSubBridge(sharedSocket)
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
