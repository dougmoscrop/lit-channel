import { LitElement, css, html } from 'lit'
import { initConnection, subscribe, publish } from './connection.js'

export class D2LChannel extends LitElement {
	static properties = {
		name: { type: String, reflect: true },
		description: { type: String },
		messages: { type: Array, state: true },
	}

	static styles = css`
		:host {
			display: block;
			box-sizing: border-box;
			font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
		}

		.container {
			border: 1px solid rgba(0, 0, 0, 0.15);
			border-radius: 10px;
			padding: 12px 14px;
		}

		header {
			display: flex;
			align-items: baseline;
			gap: 10px;
			margin-bottom: 8px;
		}

		.title {
			font-size: 16px;
			font-weight: 650;
			line-height: 1.2;
		}

		.desc {
			color: rgba(0, 0, 0, 0.7);
			font-size: 13px;
			line-height: 1.3;
		}

		.content ::slotted(*) {
			margin-top: 0;
		}

		.messages {
			list-style: none;
			margin: 8px 0 0;
			padding: 0;
		}

		.messages li {
			padding: 4px 0;
			border-top: 1px solid rgba(0, 0, 0, 0.08);
			font-size: 14px;
		}

		.compose {
			display: flex;
			gap: 6px;
			margin-top: 10px;
		}

		.compose input {
			flex: 1;
			padding: 6px 8px;
			border: 1px solid rgba(0, 0, 0, 0.2);
			border-radius: 6px;
			font-size: 14px;
		}

		.compose button {
			padding: 6px 12px;
			border: none;
			border-radius: 6px;
			background: #006fbf;
			color: #fff;
			font-size: 14px;
			cursor: pointer;
		}
	`

	_unsubscribe = null
	_connectionReady = null

	constructor() {
		super()
		this.name = 'Channel'
		this.description = ''
		this.messages = []
		this._connectionReady = initConnection()
	}

	connectedCallback() {
		super.connectedCallback()
		this._connectionReady.then(() => {
			this._unsubscribe = subscribe(this.name, (payload) => {
				this.messages = [...this.messages, payload]
				this.dispatchEvent(new CustomEvent('d2l-channel-message', {
					detail: { topic: this.name, payload },
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

	_onSend(e) {
		e.preventDefault()
		const input = this.shadowRoot.querySelector('input')
		const text = input.value.trim()
		if (!text) return
		const payload = { text, ts: Date.now() }
		publish(this.name, payload)
		// show locally too
		this.messages = [...this.messages, payload]
		this.dispatchEvent(new CustomEvent('d2l-channel-send', {
			detail: { topic: this.name, payload },
			bubbles: true,
			composed: true,
		}))
		input.value = ''
	}

	render() {
		const ariaLabel = this.name?.trim() || 'Channel'

		return html`
			<section class="container" role="region" aria-label=${ariaLabel}>
				<header>
					<div class="title">${this.name}</div>
					${this.description
						? html`<div class="desc">${this.description}</div>`
						: null}
				</header>
				<div class="content">
					<slot></slot>
				</div>
				${this.messages.length
					? html`<ul class="messages">
						${this.messages.map(m => html`<li>${m.text}</li>`)}
					</ul>`
					: null}
				<form class="compose" @submit=${this._onSend}>
					<input type="text" placeholder="Type a message…" />
					<button type="submit">Send</button>
				</form>
			</section>
		`
	}
}

customElements.define('d2l-channel', D2LChannel)
