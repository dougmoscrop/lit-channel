import { expect } from '@esm-bundle/chai'
import { PubSubBridge } from '../../src/PubSubBridge.js'

describe('PubSubBridge behavior gaps', () => {
	function createConnection() {
		const posted = []
		const eventListeners = new Map()
		return {
			posted,
			emit(type, detail) {
				const listeners = eventListeners.get(type) || []
				for (const listener of listeners) {
					listener({ detail })
				}
			},
			connection: {
				onmessage: null,
				postMessage(message) {
					posted.push(message)
				},
				addEventListener(type, listener) {
					if (!eventListeners.has(type)) {
						eventListeners.set(type, [])
					}
					eventListeners.get(type).push(listener)
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

	it('should preserve legacy subscribe and delivery behavior when resume is disabled', () => {
		const { connection, posted } = createConnection()
		const bridge = new PubSubBridge(connection)
		const received = []

		bridge.subscribe('legacy-topic', (payload) => {
			received.push(payload)
		})

		connection.onmessage({
			data: {
				type: 'message',
				topic: 'legacy-topic',
				payload: { value: 1, __rt: { streamSeq: 10, eventId: 'legacy-event' } },
			},
		})

		connection.onmessage({
			data: {
				type: 'message',
				topic: 'legacy-topic',
				payload: { value: 2, __rt: { streamSeq: 10, eventId: 'legacy-event' } },
			},
		})

		expect(posted).to.deep.equal([{ type: 'subscribe', topic: 'legacy-topic' }])
		expect(received.map((payload) => payload.value)).to.deep.equal([1, 2])
	})

	it('should include resume on subscribe only when a cursor is known', () => {
		const { connection, posted } = createConnection()
		const bridge = new PubSubBridge(connection, {
			resumeEnabled: true,
			sessionId: 'session-a',
			getResumeCursor(topic) {
				return topic === 'resume-topic'
					? { streamSeq: 7, cursor: 'cursor-7' }
					: undefined
			},
		})

		bridge.subscribe('resume-topic', () => {})
		bridge.subscribe('fresh-topic', () => {})

		expect(bridge.resumeEnabled).to.equal(true)
		expect(bridge.sessionId).to.equal('session-a')
		expect(posted).to.deep.equal([
			{
				type: 'subscribe',
				topic: 'resume-topic',
				resume: { streamSeq: 7, cursor: 'cursor-7', sessionId: 'session-a' },
			},
			{ type: 'subscribe', topic: 'fresh-topic' },
		])
	})

	it('should auto-generate a session id when resume is enabled', () => {
		const { connection } = createConnection()
		const bridge = new PubSubBridge(connection, { resumeEnabled: true })

		expect(bridge.sessionId).to.be.a('string')
		expect(bridge.sessionId.length).to.be.greaterThan(0)
	})

	it('should ack valid stream sequences and ignore invalid ones', () => {
		const { connection, posted } = createConnection()
		const bridge = new PubSubBridge(connection, { resumeEnabled: true, sessionId: 'ack-session' })

		bridge.subscribe('ack-topic', () => {})
		posted.length = 0

		connection.onmessage({
			data: {
				type: 'message',
				topic: 'ack-topic',
				payload: { value: 'ok', __rt: { streamSeq: 4 } },
			},
		})
		connection.onmessage({
			data: {
				type: 'message',
				topic: 'ack-topic',
				payload: { value: 'bad', __rt: { streamSeq: 'not-a-number' } },
			},
		})
		connection.onmessage({
			data: {
				type: 'message',
				topic: 'ack-topic',
				payload: { value: 'missing', __rt: {} },
			},
		})

		expect(posted).to.deep.equal([
			{ type: 'ack', topic: 'ack-topic', streamSeq: 4, cursor: '4', sessionId: 'ack-session' },
		])
	})

	it('should keep ack cursors monotonic per topic', () => {
		const { connection, posted } = createConnection()
		const bridge = new PubSubBridge(connection, { resumeEnabled: true, sessionId: 'mono-session' })

		bridge.subscribe('mono-topic', () => {})
		posted.length = 0

		for (const streamSeq of [10, 9, 10, 11]) {
			connection.onmessage({
				data: {
					type: 'message',
					topic: 'mono-topic',
					payload: { __rt: { streamSeq } },
				},
			})
		}

		expect(posted).to.deep.equal([
			{ type: 'ack', topic: 'mono-topic', streamSeq: 10, cursor: '10', sessionId: 'mono-session' },
			{ type: 'ack', topic: 'mono-topic', streamSeq: 11, cursor: '11', sessionId: 'mono-session' },
		])
	})

	it('should suppress duplicate eventIds while delivering unique events', () => {
		const { connection } = createConnection()
		const bridge = new PubSubBridge(connection, { resumeEnabled: true, sessionId: 'dedupe-session' })
		const received = []

		bridge.subscribe('dedupe-topic', (payload) => {
			received.push(payload.value)
		})

		connection.onmessage({
			data: {
				type: 'message',
				topic: 'dedupe-topic',
				payload: { value: 'first', __rt: { streamSeq: 1, eventId: 'event-a' } },
			},
		})
		connection.onmessage({
			data: {
				type: 'message',
				topic: 'dedupe-topic',
				payload: { value: 'duplicate', __rt: { streamSeq: 2, eventId: 'event-a' } },
			},
		})
		connection.onmessage({
			data: {
				type: 'message',
				topic: 'dedupe-topic',
				payload: { value: 'second', __rt: { streamSeq: 3, eventId: 'event-b' } },
			},
		})

		expect(received).to.deep.equal(['first', 'second'])
	})

	it('should evict old eventIds from the bounded dedupe cache', () => {
		const { connection } = createConnection()
		const bridge = new PubSubBridge(connection, {
			resumeEnabled: true,
			sessionId: 'evict-session',
			eventIdDedupeLimit: 2,
		})
		const received = []

		bridge.subscribe('evict-topic', (payload) => {
			received.push(payload.value)
		})

		for (const payload of [
			{ value: 'a-1', __rt: { streamSeq: 1, eventId: 'a' } },
			{ value: 'b', __rt: { streamSeq: 2, eventId: 'b' } },
			{ value: 'a-duplicate', __rt: { streamSeq: 3, eventId: 'a' } },
			{ value: 'c', __rt: { streamSeq: 4, eventId: 'c' } },
			{ value: 'a-2', __rt: { streamSeq: 5, eventId: 'a' } },
		]) {
			connection.onmessage({
				data: { type: 'message', topic: 'evict-topic', payload },
			})
		}

		expect(received).to.deep.equal(['a-1', 'b', 'c', 'a-2'])
	})

	it('should re-subscribe with the best known cursor after reconnect', async () => {
		const { connection, posted, emit } = createConnection()
		const bridge = new PubSubBridge(connection, { resumeEnabled: true, sessionId: 'reconnect-session' })

		bridge.subscribe('reconnect-topic', () => {})
		posted.length = 0

		connection.onmessage({
			data: {
				type: 'message',
				topic: 'reconnect-topic',
				payload: { __rt: { streamSeq: 12 } },
			},
		})
		posted.length = 0

		emit('reconnected')
		await new Promise(resolve => setTimeout(resolve, 130))

		expect(posted).to.deep.equal([
			{
				type: 'subscribe',
				topic: 'reconnect-topic',
				resume: { streamSeq: 12, cursor: '12', sessionId: 'reconnect-session' },
			},
		])
	})
})
