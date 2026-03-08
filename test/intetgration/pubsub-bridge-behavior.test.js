import { expect } from '@esm-bundle/chai'
import { PubSubBridge } from '../../src/PubSubBridge.js'

describe('PubSubBridge behavior gaps', () => {
	function createConnection() {
		const posted = []
		return {
			posted,
			connection: {
				onmessage: null,
				postMessage(message) {
					posted.push(message)
				},
			},
		}
	}

	it('should throw when constructed without a postMessage-capable connection', () => {
		expect(() => new PubSubBridge()).to.throw('connection must have a postMessage method')
		expect(() => new PubSubBridge({})).to.throw('connection must have a postMessage method')
		expect(() => new PubSubBridge({ postMessage: 42 })).to.throw('connection must have a postMessage method')
	})

	it('should preserve original onmessage handler while routing to subscribers', () => {
		const { connection } = createConnection()
		const seenByOriginalHandler = []
		const seenBySubscriber = []

		connection.onmessage = (event) => {
			seenByOriginalHandler.push(event.data)
		}

		const bridge = new PubSubBridge(connection)
		bridge.subscribe('topic-a', (payload, topic) => {
			seenBySubscriber.push({ payload, topic })
		})

		connection.onmessage({
			data: { type: 'message', topic: 'topic-a', payload: { id: 1 } },
		})

		expect(seenByOriginalHandler).to.have.lengthOf(1)
		expect(seenByOriginalHandler[0]).to.deep.equal({ type: 'message', topic: 'topic-a', payload: { id: 1 } })
		expect(seenBySubscriber).to.deep.equal([{ payload: { id: 1 }, topic: 'topic-a' }])
	})

	it('should respond to ping with pong envelope', () => {
		const { connection, posted } = createConnection()
		new PubSubBridge(connection)

		connection.onmessage({ data: { type: 'ping' } })

		expect(posted).to.deep.equal([{ type: 'pong' }])
	})

	it('should only send unsubscribe after the last callback is removed', () => {
		const { connection, posted } = createConnection()
		const bridge = new PubSubBridge(connection)

		const unsubA = bridge.subscribe('shared-topic', () => {})
		const unsubB = bridge.subscribe('shared-topic', () => {})

		unsubA()
		expect(posted.filter((msg) => msg.type === 'unsubscribe')).to.have.lengthOf(0)

		unsubB()
		expect(posted.filter((msg) => msg.type === 'unsubscribe')).to.deep.equal([
			{ type: 'unsubscribe', topic: 'shared-topic' },
		])
	})

	it('should emit publish envelopes through connection', () => {
		const { connection, posted } = createConnection()
		const bridge = new PubSubBridge(connection)

		bridge.publish('send-topic', { hello: 'world' })

		expect(posted).to.deep.equal([
			{ type: 'publish', topic: 'send-topic', payload: { hello: 'world' } },
		])
	})
})
