import { expect } from '@esm-bundle/chai'
import { SharedSocket } from '../../src/SharedSocket.js'
import { PubSubBridge } from '../../src/PubSubBridge.js'

function createFakeWebSocketClass() {
	class FakeWebSocket {
		static CONNECTING = 0
		static OPEN = 1
		static CLOSED = 3
		static instances = []

		constructor(url, protocols) {
			this.url = url
			this.protocols = protocols
			this.readyState = FakeWebSocket.CONNECTING
			this.sent = []
			this.closeCalls = 0
			this.listeners = new Map()
			FakeWebSocket.instances.push(this)
		}

		addEventListener(type, callback) {
			if (!this.listeners.has(type)) {
				this.listeners.set(type, [])
			}
			this.listeners.get(type).push(callback)
		}

		send(data) {
			this.sent.push(data)
		}

		close() {
			this.closeCalls += 1
			this.readyState = FakeWebSocket.CLOSED
			this.emit('close')
		}

		emit(type, event = {}) {
			const handlers = this.listeners.get(type) || []
			for (const cb of handlers) {
				cb(event)
			}
		}

		emitOpen() {
			this.readyState = FakeWebSocket.OPEN
			this.emit('open')
		}

		emitError() {
			this.emit('error')
		}
	}

	return FakeWebSocket
}

async function waitFor(check, timeoutMs = 2000) {
	const start = Date.now()
	while (!check()) {
		if (Date.now() - start > timeoutMs) {
			throw new Error('Timed out waiting for condition')
		}
		await new Promise((resolve) => setTimeout(resolve, 10))
	}
}

describe('BroadcastChannel transport resilience', () => {
	let OriginalSharedWorker
	let OriginalWebSocket
	let OriginalSetTimeout
	let OriginalClearTimeout

	beforeEach(() => {
		OriginalSharedWorker = window.SharedWorker
		OriginalWebSocket = window.WebSocket
		OriginalSetTimeout = window.setTimeout
		OriginalClearTimeout = window.clearTimeout
	})

	afterEach(() => {
		window.SharedWorker = OriginalSharedWorker
		window.WebSocket = OriginalWebSocket
		window.setTimeout = OriginalSetTimeout
		window.clearTimeout = OriginalClearTimeout
	})

	it('should schedule reconnect after websocket close in BroadcastChannel fallback', async () => {
		const FakeWebSocket = createFakeWebSocketClass()
		const scheduledDelays = []

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)
		window.setTimeout = /** @type {any} */ ((fn, delay, ...args) => {
			scheduledDelays.push(delay)
			return OriginalSetTimeout(() => fn(...args), 0)
		})

		const socket = new SharedSocket()
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			FakeWebSocket.instances[0].emit('close')
			await waitFor(() => FakeWebSocket.instances.length >= 2)

			expect(scheduledDelays).to.include(1000)
		} finally {
			await socket.close()
		}
	})

	it('should close websocket when error event is emitted', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)

		const socket = new SharedSocket()
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			const ws = FakeWebSocket.instances[0]
			ws.emitError()

			expect(ws.closeCalls).to.equal(1)
		} finally {
			await socket.close()
		}
	})

	it('should re-subscribe topics after reconnect and open', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)
		window.setTimeout = /** @type {any} */ ((fn, _delay, ...args) => {
			return OriginalSetTimeout(() => fn(...args), 0)
		})

		const socket = new SharedSocket()
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			const bridge = new PubSubBridge(socket)
			const unsubscribe = bridge.subscribe('resub-topic', () => {})

			const firstWs = FakeWebSocket.instances[0]
			firstWs.emitOpen()

			const firstOpenMessages = firstWs.sent.map((raw) => JSON.parse(raw))
			expect(firstOpenMessages).to.deep.include({ type: 'subscribe', topic: 'resub-topic' })

			firstWs.emit('close')
			await waitFor(() => FakeWebSocket.instances.length >= 2)

			const secondWs = FakeWebSocket.instances[1]
			secondWs.emitOpen()
			const secondOpenMessages = secondWs.sent.map((raw) => JSON.parse(raw))
			expect(secondOpenMessages).to.deep.include({ type: 'subscribe', topic: 'resub-topic' })

			unsubscribe()
		} finally {
			await socket.close()
		}
	})

	it('should forward resume subscribe and ack frames in BroadcastChannel fallback', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)

		const socket = new SharedSocket()
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			const bridge = new PubSubBridge(socket, {
				resumeEnabled: true,
				sessionId: 'bc-session',
				getResumeCursor(topic) {
					return topic === 'bc-resume-topic'
						? { streamSeq: 5, cursor: 'cursor-5' }
						: undefined
				},
			})
			bridge.subscribe('bc-resume-topic', () => {})

			const ws = FakeWebSocket.instances[0]
			ws.emitOpen()

			const openMessages = ws.sent.map((raw) => JSON.parse(raw))
			expect(openMessages).to.deep.include({
				type: 'subscribe',
				topic: 'bc-resume-topic',
				resume: { streamSeq: 5, cursor: 'cursor-5', sessionId: 'bc-session' },
			})

			ws.sent.length = 0
			ws.emit('message', {
				data: JSON.stringify({
					type: 'message',
					topic: 'bc-resume-topic',
					payload: { __rt: { streamSeq: 6, eventId: 'bc-event-6' } },
				}),
			})

			const ackMessages = ws.sent.map((raw) => JSON.parse(raw))
			expect(ackMessages).to.deep.equal([
				{ type: 'ack', topic: 'bc-resume-topic', streamSeq: 6, cursor: '6', sessionId: 'bc-session' },
			])
		} finally {
			await socket.close()
		}
	})

	it('should relay subscribed ACKs to leader and follower bridges in BroadcastChannel fallback', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)

		const socketA = new SharedSocket()
		const socketB = new SharedSocket()
		try {
			await socketA.connect()
			await socketB.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			const topic = `bc-subscribed-${Date.now()}`
			const bridgeA = new PubSubBridge(socketA)
			const bridgeB = new PubSubBridge(socketB)
			const unsubscribeA = bridgeA.subscribe(topic, () => {})
			const unsubscribeB = bridgeB.subscribe(topic, () => {})

			const ackA = bridgeA.waitForSubscribed(topic, { timeout: 1000 })
			const ackB = bridgeB.waitForSubscribed(topic, { timeout: 1000 })
			FakeWebSocket.instances[0].emitOpen()
			FakeWebSocket.instances[0].emit('message', {
				data: JSON.stringify({ type: 'subscribed', topic, resume: { accepted: true } }),
			})

			expect(await ackA).to.deep.equal({ type: 'subscribed', topic, resume: { accepted: true } })
			expect(await ackB).to.deep.equal({ type: 'subscribed', topic, resume: { accepted: true } })

			unsubscribeA()
			unsubscribeB()
		} finally {
			await socketA.close()
			await socketB.close()
		}
	})

	it('should relay replay and error control frames in BroadcastChannel fallback', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)

		const socket = new SharedSocket()
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			const bridge = new PubSubBridge(socket)
			const replayEvents = []
			const errorEvents = []
			bridge.addEventListener('replay-complete', (event) => replayEvents.push(event.detail))
			bridge.addEventListener('error', (event) => errorEvents.push(event.detail))
			bridge.subscribe('bc-control-topic', () => {})
			FakeWebSocket.instances[0].emitOpen()

			FakeWebSocket.instances[0].emit('message', {
				data: JSON.stringify({ type: 'replay-complete', topic: 'bc-control-topic', fromSeq: 3, toSeq: 5 }),
			})
			FakeWebSocket.instances[0].emit('message', {
				data: JSON.stringify({ type: 'error', topic: 'bc-control-topic', error: 'forbidden' }),
			})
			FakeWebSocket.instances[0].emit('message', {
				data: JSON.stringify({ type: 'error', error: 'global' }),
			})

			expect(replayEvents).to.deep.equal([
				{ type: 'replay-complete', topic: 'bc-control-topic', fromSeq: 3, toSeq: 5 },
			])
			expect(errorEvents).to.deep.equal([
				{ type: 'error', topic: 'bc-control-topic', error: 'forbidden' },
				{ type: 'error', error: 'global' },
			])
		} finally {
			await socket.close()
		}
	})

	it('should replay resume cursor when BroadcastChannel websocket reconnects', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)
		window.setTimeout = /** @type {any} */ ((fn, _delay, ...args) => {
			return OriginalSetTimeout(() => fn(...args), 0)
		})

		const socket = new SharedSocket()
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			const bridge = new PubSubBridge(socket, { resumeEnabled: true, sessionId: 'bc-reconnect-session' })
			bridge.subscribe('bc-reconnect-topic', () => {})

			const firstWs = FakeWebSocket.instances[0]
			firstWs.emitOpen()
			firstWs.emit('message', {
				data: JSON.stringify({
					type: 'message',
					topic: 'bc-reconnect-topic',
					payload: { __rt: { streamSeq: 9 } },
				}),
			})

			firstWs.emit('close')
			await waitFor(() => FakeWebSocket.instances.length >= 2)

			const secondWs = FakeWebSocket.instances[1]
			secondWs.emitOpen()
			const secondOpenMessages = secondWs.sent.map((raw) => JSON.parse(raw))
			expect(secondOpenMessages).to.deep.include({
				type: 'subscribe',
				topic: 'bc-reconnect-topic',
				resume: { streamSeq: 9, cursor: '9', sessionId: 'bc-reconnect-session' },
			})
		} finally {
			await socket.close()
		}
	})

	it('should pass bearer token to websocket in BroadcastChannel fallback', async () => {
		const FakeWebSocket = createFakeWebSocketClass()

		window.SharedWorker = undefined
		window.WebSocket = /** @type {any} */ (FakeWebSocket)

		const socket = new SharedSocket({ authToken: 'Bearer fallback-token' })
		try {
			await socket.connect()
			await waitFor(() => FakeWebSocket.instances.length >= 1)

			expect(FakeWebSocket.instances[0].protocols).to.deep.equal(['bearer', 'fallback-token'])
		} finally {
			await socket.close()
		}
	})
})
