import { expect } from '@esm-bundle/chai'
import { html, fixture } from '@open-wc/testing'
import { getConfiguredEndpoint, resolveWebSocketUrl } from '../../src/SharedSocket.js'
import { __resetConnectionForTests } from '../../src/lit-channel.js'
import '../../src/lit-channel.js'

describe('Endpoint config', () => {
	afterEach(async () => {
		document.head.querySelector('meta[name="lit-channel-endpoint"]')?.remove()
		await __resetConnectionForTests()
	})

	it('should read endpoint from head meta', () => {
		const meta = document.createElement('meta')
		meta.setAttribute('name', 'lit-channel-endpoint')
		meta.setAttribute('content', ' /custom/ws ')
		document.head.appendChild(meta)

		expect(getConfiguredEndpoint(document)).to.equal('/custom/ws')
	})

	it('should resolve relative endpoint to ws URL', () => {
		const locationLike = {
			protocol: 'https:',
			host: 'example.com',
			href: 'https://example.com/app/index.html',
		}

		expect(resolveWebSocketUrl('/socket', locationLike)).to.equal('wss://example.com/socket')
	})

	it('should convert absolute http endpoint to ws URL', () => {
		const locationLike = {
			protocol: 'https:',
			host: 'example.com',
			href: 'https://example.com/app/index.html',
		}

		expect(resolveWebSocketUrl('http://backend.example.com/live', locationLike))
			.to.equal('ws://backend.example.com/live')
	})

	it('should pass meta endpoint to SharedWorker config', async () => {
		const meta = document.createElement('meta')
		meta.setAttribute('name', 'lit-channel-endpoint')
		meta.setAttribute('content', '/configured/ws')
		document.head.appendChild(meta)

		const OriginalSharedWorker = window.SharedWorker
		const postedMessages = []

		window.SharedWorker = class {
			constructor() {
				this.port = {
					onmessage: null,
					start() {},
					postMessage(message) {
						postedMessages.push(message)
					},
					close() {},
				}
			}
		}

		let element = null
		try {
			element = await fixture(html`<lit-channel name="endpoint-test"></lit-channel>`)

			const started = Date.now()
			while (!element._bridge) {
				if (Date.now() - started > 2000) {
					throw new Error('Timed out waiting for bridge')
				}
				await element.updateComplete
				await Promise.resolve()
			}

			expect(postedMessages).to.deep.include({
				type: 'config',
				endpoint: `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/configured/ws`,
			})
		} finally {
			if (element?.parentElement) {
				element.parentElement.removeChild(element)
			}
			window.SharedWorker = OriginalSharedWorker
		}
	})
})
