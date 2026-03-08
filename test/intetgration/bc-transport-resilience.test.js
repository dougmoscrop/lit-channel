import { expect } from '@esm-bundle/chai'
import { SharedSocket } from '../../src/SharedSocket.js'
import { PubSubBridge } from '../../src/PubSubBridge.js'

function createFakeWebSocketClass() {
	class FakeWebSocket {
		static CONNECTING = 0
		static OPEN = 1
		static CLOSED = 3
		static instances = []

		constructor(url) {
			this.url = url
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

			expect(scheduledDelays).to.include(3000)
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
})
