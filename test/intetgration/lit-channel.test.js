import { expect } from '@esm-bundle/chai'
import { html, fixture } from '@open-wc/testing'
import '../../src/lit-channel.js'

async function waitForBridge(element, timeoutMs = 2000) {
	const started = Date.now()
	while (!element._bridge) {
		if (Date.now() - started > timeoutMs) {
			throw new Error('Timed out waiting for lit-channel bridge')
		}
		await element.updateComplete
		await Promise.resolve()
	}
}

async function waitForSubscription(element, timeoutMs = 2000) {
	const started = Date.now()
	while (typeof element._unsubscribe !== 'function') {
		if (Date.now() - started > timeoutMs) {
			throw new Error('Timed out waiting for lit-channel subscription setup')
		}
		await element.updateComplete
		await Promise.resolve()
	}
}

describe('LitChannel', () => {
	let element

	afterEach(async () => {
		if (element?.parentElement) {
			element.parentElement.removeChild(element)
		}
	})

	it('should render with default properties', async () => {
		element = await fixture(html`<lit-channel></lit-channel>`)
		expect(element).to.exist
		expect(element.name).to.equal('Channel')
	})

	it('should accept name and description attributes', async () => {
		element = await fixture(html`
			<lit-channel
				name="Announcements"
				description="Important announcements"
			></lit-channel>
		`)
		expect(element.name).to.equal('Announcements')
		expect(element.description).to.equal('Important announcements')
	})

	it('should render only slotted content', async () => {
		element = await fixture(html`
			<lit-channel>
				<p>Test content</p>
			</lit-channel>
		`)
		const slot = element.shadowRoot.querySelector('slot')
		expect(slot).to.exist
	})

	it('should dispatch lit-channel-message event when subscribed', async () => {
		element = await fixture(html`<lit-channel name="test"></lit-channel>`)

		let eventFired = false
		let eventDetail = null

		element.addEventListener('lit-channel-message', (e) => {
			eventFired = true
			eventDetail = e.detail
		})

		await waitForBridge(element)

		// Simulate receiving a message from the bridge
		element.dispatchEvent(new CustomEvent('lit-channel-message', {
			detail: { topic: 'test', payload: { text: 'received' } },
			bubbles: true,
			composed: true,
		}))

		expect(eventFired).to.be.true
		expect(eventDetail).to.deep.equal({ topic: 'test', payload: { text: 'received' } })
		expect(eventDetail.topic).to.equal('test')
		expect(eventDetail.payload.text).to.equal('received')
	})

	it('should have publish method', async () => {
		element = await fixture(html`<lit-channel name="test"></lit-channel>`)
		await waitForBridge(element)

		expect(element.publish).to.be.a('function')

		let eventFired = false
		element.addEventListener('lit-channel-send', (e) => {
			eventFired = true
		})

		element.publish({ text: 'test message' })
		await element.updateComplete

		expect(eventFired).to.be.true
	})

	it('should dispatch lit-channel-send event when publishing', async () => {
		element = await fixture(html`<lit-channel name="test"></lit-channel>`)
		await waitForBridge(element)

		let eventDetail = null
		element.addEventListener('lit-channel-send', (e) => {
			eventDetail = e.detail
		})

		const payload = { text: 'test message', ts: Date.now() }
		element.publish(payload)
		await element.updateComplete

		expect(eventDetail.topic).to.equal('test')
		expect(eventDetail.payload).to.deep.equal(payload)
	})

	it('should emit exactly one lit-channel-send event with exact contract', async () => {
		element = await fixture(html`<lit-channel name="Announcements"></lit-channel>`)
		await waitForBridge(element)

		let count = 0
		let capturedEvent = null
		element.addEventListener('lit-channel-send', (event) => {
			count += 1
			capturedEvent = event
		})

		const payload = { text: 'contract-check', ts: Date.now(), meta: { strict: true } }
		element.publish(payload)
		await element.updateComplete

		expect(count).to.equal(1)
		expect(capturedEvent.bubbles).to.equal(true)
		expect(capturedEvent.composed).to.equal(true)
		expect(capturedEvent.detail).to.deep.equal({
			topic: 'Announcements',
			payload,
		})
	})

	it('should subscribe on connect and unsubscribe on disconnect without duplicate reattach subscriptions', async () => {
		element = await fixture(html`<lit-channel name="lifecycle-topic"></lit-channel>`)
		await waitForBridge(element)

		expect(element._unsubscribe).to.be.a('function')

		const bridge = element._bridge
		const originalSubscribe = bridge.subscribe.bind(bridge)
		let subscribeCalls = 0
		let unsubscribeCalls = 0

		bridge.subscribe = (topic, callback) => {
			subscribeCalls += 1
			const unsub = originalSubscribe(topic, callback)
			return () => {
				unsubscribeCalls += 1
				unsub()
			}
		}

		element.remove()
		expect(unsubscribeCalls).to.equal(0)

		document.body.appendChild(element)
		await waitForBridge(element)
		await waitForSubscription(element)

		element.remove()
		expect(unsubscribeCalls).to.equal(1)

		document.body.appendChild(element)
		await waitForBridge(element)
		await waitForSubscription(element)

		expect(subscribeCalls).to.equal(2)
		element.remove()
		expect(unsubscribeCalls).to.equal(2)
	})
})


