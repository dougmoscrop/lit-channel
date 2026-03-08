import { expect } from '@esm-bundle/chai'
import { html, fixture } from '@open-wc/testing'
import { __resetConnectionForTests } from '../../src/lit-channel.js'
import '../../src/lit-channel.js'

async function waitForBridge(element, timeoutMs = 2000) {
	const start = Date.now()
	while (!element._bridge) {
		if (Date.now() - start > timeoutMs) {
			throw new Error('Timed out waiting for bridge')
		}
		await element.updateComplete
		await Promise.resolve()
	}
}

describe('lit-channel singleton and lifecycle', () => {
	let OriginalSharedWorker

	beforeEach(() => {
		OriginalSharedWorker = window.SharedWorker
	})

	afterEach(async () => {
		window.SharedWorker = OriginalSharedWorker
		await __resetConnectionForTests()
	})

	it('should share one connection bridge across multiple elements', async () => {
		let workerConstructCount = 0
		const postedMessages = []
		let capturedPort = null

		window.SharedWorker = /** @type {any} */ (class {
			constructor() {
				workerConstructCount += 1
				capturedPort = {
					onmessage: null,
					start() {},
					postMessage(msg) { postedMessages.push(msg) },
					close() {},
				}
				this.port = capturedPort
			}
		})

		const first = /** @type {any} */ (await fixture(html`<lit-channel name="TopicA"></lit-channel>`))
		const second = /** @type {any} */ (await fixture(html`<lit-channel name="TopicB"></lit-channel>`))

		try {
			await waitForBridge(first)
			await waitForBridge(second)

			expect(workerConstructCount).to.equal(1)
			expect(first._bridge).to.equal(second._bridge)
			expect(postedMessages.filter((msg) => msg.type === 'config')).to.have.lengthOf(1)

			let firstReceived = null
			let secondReceived = null
			first.addEventListener('lit-channel-message', (event) => {
				firstReceived = event.detail
			})
			second.addEventListener('lit-channel-message', (event) => {
				secondReceived = event.detail
			})

			;(/** @type {any} */ (capturedPort)).onmessage?.({
				data: {
					type: 'message',
					topic: 'TopicA',
					payload: { text: 'hello-topic-a' },
				},
			})

			expect(firstReceived).to.deep.equal({ topic: 'TopicA', payload: { text: 'hello-topic-a' } })
			expect(secondReceived).to.equal(null)
		} finally {
			first.remove()
			second.remove()
		}
	})

	it('should send one unsubscribe for a shared topic only after last element disconnects', async () => {
		const postedMessages = []

		window.SharedWorker = /** @type {any} */ (class {
			constructor() {
				this.port = {
					onmessage: null,
					start() {},
					postMessage(msg) { postedMessages.push(msg) },
					close() {},
				}
			}
		})

		const first = await fixture(html`<lit-channel name="SameTopic"></lit-channel>`)
		const second = await fixture(html`<lit-channel name="SameTopic"></lit-channel>`)

		try {
			await waitForBridge(first)
			await waitForBridge(second)

			const subscribeCount = postedMessages.filter((msg) => (
				msg.type === 'subscribe' && msg.topic === 'SameTopic'
			)).length
			expect(subscribeCount).to.equal(2)

			first.remove()
			await Promise.resolve()

			const unsubAfterFirstRemove = postedMessages.filter((msg) => (
				msg.type === 'unsubscribe' && msg.topic === 'SameTopic'
			)).length
			expect(unsubAfterFirstRemove).to.equal(0)

			second.remove()
			await Promise.resolve()

			const unsubAfterSecondRemove = postedMessages.filter((msg) => (
				msg.type === 'unsubscribe' && msg.topic === 'SameTopic'
			)).length
			expect(unsubAfterSecondRemove).to.equal(1)
		} finally {
			first.remove()
			second.remove()
		}
	})

	it('should warn and avoid dispatching send events if publish is called before bridge readiness', () => {
		const element = /** @type {any} */ (document.createElement('lit-channel'))
		element.name = 'PreReady'

		let sendCount = 0
		let warningCount = 0
		const originalWarn = console.warn
		console.warn = () => {
			warningCount += 1
		}

		element.addEventListener('lit-channel-send', () => {
			sendCount += 1
		})

		try {
			element.publish({ text: 'not-ready' })
			expect(warningCount).to.equal(1)
			expect(sendCount).to.equal(0)
		} finally {
			console.warn = originalWarn
		}
	})
})
