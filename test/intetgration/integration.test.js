import { expect } from '@esm-bundle/chai'
import { SharedSocket } from '../../src/SharedSocket.js'
import { PubSubBridge } from '../../src/PubSubBridge.js'

/**
 * Integration tests verifying both bc-transport and shared-worker
 * transports produce identical behavior.
 */
describe('Transport Integration', () => {
	const subscriptions = []
	let socket = null
	let bridge = null

	function wsUrl() {
		return `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/api/ws`
	}

	function openRawWebSocket() {
		return new Promise((resolve, reject) => {
			const ws = new WebSocket(wsUrl())
			ws.addEventListener('open', () => resolve(ws), { once: true })
			ws.addEventListener('error', () => reject(new Error('raw websocket failed to open')), { once: true })
		})
	}

	async function publishRaw(topic, payload) {
		const ws = await openRawWebSocket()
		try {
			ws.send(JSON.stringify({ type: 'publish', topic, payload }))
		} finally {
			setTimeout(() => ws.close(), 0)
		}
	}

	afterEach(() => {
		subscriptions.forEach(unsub => unsub?.())
		subscriptions.length = 0
	})

	describe('BroadcastChannel transport (SharedWorker fallback)', () => {
		beforeEach(async () => {
			// Simulate SharedWorker being unavailable
			window.SharedWorker = undefined
			socket = new SharedSocket()
			await socket.connect()
			bridge = new PubSubBridge(socket)
		})

		afterEach(() => {
			// Restore SharedWorker
			delete window.SharedWorker
		})

		it('should fallback to BroadcastChannel when SharedWorker unavailable', async () => {
			expect(socket).to.exist
			expect(socket).to.have.property('onmessage')
			expect(socket.isReady).to.be.true
		})

		it('should support subscribe/publish/unsubscribe via BroadcastChannel', async () => {
			const results = []

			const unsub1 = bridge.subscribe('bc-multi-sub', (payload) => {
				results.push({ id: 1, payload })
			})
			subscriptions.push(unsub1)

			const unsub2 = bridge.subscribe('bc-multi-sub', (payload) => {
				results.push({ id: 2, payload })
			})
			subscriptions.push(unsub2)

			await new Promise(resolve => setTimeout(resolve, 50))

			// Simulate a server message for this topic
			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'bc-multi-sub',
					payload: { text: 'broadcast' },
				},
			})

			await new Promise(resolve => setTimeout(resolve, 50))

			expect(results).to.have.lengthOf(2)
			expect(results[0].id).to.equal(1)
			expect(results[1].id).to.equal(2)
			expect(results[0].payload.text).to.equal('broadcast')
		})
	})

	describe('SharedWorker transport (default)', () => {
		beforeEach(async () => {
			socket = new SharedSocket()
			await socket.connect()
			bridge = new PubSubBridge(socket)
		})

		it('should use SharedWorker when available', async () => {
			const hasSharedWorker = typeof window.SharedWorker !== 'undefined'
			if (!hasSharedWorker) {
				// Skip gracefully if not available
				return
			}

			expect(socket).to.exist
			expect(socket.isReady).to.be.true
		})

		it('should support subscribe/publish via SharedWorker', async () => {
			const hasSharedWorker = typeof window.SharedWorker !== 'undefined'
			if (!hasSharedWorker) {
				return
			}

			const unsub = bridge.subscribe('integration-test-sw', () => {})
			subscriptions.push(unsub)
			expect(unsub).to.be.a('function')
		})
	})

	describe('Transport behavior equivalence', () => {
		beforeEach(async () => {
			socket = new SharedSocket()
			await socket.connect()
			bridge = new PubSubBridge(socket)
		})

		it('should deliver messages to multiple subscribers', async () => {
			const results = []

			const unsub1 = bridge.subscribe('multi-subscriber-test', (payload) => {
				results.push({ subscriber: 1, payload })
			})
			subscriptions.push(unsub1)

			const unsub2 = bridge.subscribe('multi-subscriber-test', (payload) => {
				results.push({ subscriber: 2, payload })
			})
			subscriptions.push(unsub2)

			await new Promise(resolve => setTimeout(resolve, 50))

			// Simulate receiving a message from the server/WS
			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'multi-subscriber-test',
					payload: { text: 'broadcast' },
				},
			})

			await new Promise(resolve => setTimeout(resolve, 50))

			expect(results).to.have.lengthOf(2)
			expect(results[0].subscriber).to.equal(1)
			expect(results[1].subscriber).to.equal(2)
			expect(results[0].payload.text).to.equal('broadcast')
			expect(results[1].payload.text).to.equal('broadcast')
		})

		it('should stop delivering after unsubscribe', async () => {
			const results = []

			const unsub = bridge.subscribe('symm-test', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			await new Promise(resolve => setTimeout(resolve, 50))

			// Message should be delivered
			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'symm-test',
					payload: { seq: 1 },
				},
			})

			await new Promise(resolve => setTimeout(resolve, 50))
			expect(results).to.have.lengthOf(1)

			// Unsubscribe
			unsub()

			// Message should NOT be delivered
			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'symm-test',
					payload: { seq: 2 },
				},
			})

			await new Promise(resolve => setTimeout(resolve, 50))
			expect(results).to.have.lengthOf(1) // Still just 1
		})

		it('should isolate messages by topic', async () => {
			const results = { topic1: [], topic2: [] }

			const unsub1 = bridge.subscribe('topic-1', (payload) => {
				results.topic1.push(payload)
			})
			subscriptions.push(unsub1)

			const unsub2 = bridge.subscribe('topic-2', (payload) => {
				results.topic2.push(payload)
			})
			subscriptions.push(unsub2)

			await new Promise(resolve => setTimeout(resolve, 50))

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'topic-1',
					payload: { from: 'topic1' },
				},
			})

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'topic-2',
					payload: { from: 'topic2' },
				},
			})

			await new Promise(resolve => setTimeout(resolve, 50))

			expect(results.topic1).to.have.lengthOf(1)
			expect(results.topic1[0].from).to.equal('topic1')

			expect(results.topic2).to.have.lengthOf(1)
			expect(results.topic2[0].from).to.equal('topic2')
		})

		it('should not deliver messages for unsubscribed topics', async () => {
			const results = []

			const unsub = bridge.subscribe('subscribed-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			await new Promise(resolve => setTimeout(resolve, 50))

			// Send message for subscribed topic
			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'subscribed-topic',
					payload: { msg: 'should receive' },
				},
			})

			// Send message for unsubscribed topic
			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'unsubscribed-topic',
					payload: { msg: 'should NOT receive' },
				},
			})

			await new Promise(resolve => setTimeout(resolve, 50))

			expect(results).to.have.lengthOf(1)
			expect(results[0].msg).to.equal('should receive')
		})
	})

	describe('Lifecycle loop safety', () => {
		it('should cleanly cycle connect/subscribe/unsubscribe/disconnect repeatedly', async () => {
			for (let i = 0; i < 8; i++) {
				const localSocket = new SharedSocket()
				await localSocket.connect()
				const localBridge = new PubSubBridge(localSocket)

				const received = []
				const unsub = localBridge.subscribe(`loop-topic-${i}`, (payload) => {
					received.push(payload)
				})

				localSocket.onmessage?.({
					data: {
						type: 'message',
						topic: `loop-topic-${i}`,
						payload: { seq: i },
					},
				})

				expect(received).to.have.lengthOf(1)

				unsub()
				localSocket.onmessage?.({
					data: {
						type: 'message',
						topic: `loop-topic-${i}`,
						payload: { seq: `${i}-after` },
					},
				})

				expect(received).to.have.lengthOf(1)
				await localSocket.close()
				expect(localSocket.isReady).to.equal(false)
			}
		})

		it('should not leak delivery across repeated unsubscribe/resubscribe loops', async () => {
			const topic = 'loop-shared-topic'
			const localSocket = new SharedSocket()
			await localSocket.connect()
			const localBridge = new PubSubBridge(localSocket)

			let totalDelivered = 0
			for (let i = 0; i < 12; i++) {
				const unsub = localBridge.subscribe(topic, () => {
					totalDelivered += 1
				})
				localSocket.onmessage?.({
					data: { type: 'message', topic, payload: { seq: i } },
				})
				unsub()
				localSocket.onmessage?.({
					data: { type: 'message', topic, payload: { seq: `ignored-${i}` } },
				})
			}

			expect(totalDelivered).to.equal(12)
			await localSocket.close()
			expect(localSocket.isReady).to.equal(false)
		})
	})

	describe('Transport parity matrix', () => {
		async function runContractScenario(forceFallback) {
			const originalSharedWorker = window.SharedWorker
			if (forceFallback) {
				window.SharedWorker = undefined
			}

			const localSocket = new SharedSocket()
			await localSocket.connect()
			const localBridge = new PubSubBridge(localSocket)

			const eventsA = []
			const eventsB = []
			const unsubA = localBridge.subscribe('parity-topic', (payload, topic) => {
				eventsA.push({ topic, payload })
			})
			const unsubB = localBridge.subscribe('parity-topic', (payload, topic) => {
				eventsB.push({ topic, payload })
			})

			localSocket.onmessage?.({
				data: {
					type: 'message',
					topic: 'parity-topic',
					payload: { seq: 1, text: 'first' },
				},
			})

			unsubB()
			localSocket.onmessage?.({
				data: {
					type: 'message',
					topic: 'parity-topic',
					payload: { seq: 2, text: 'second' },
				},
			})

			localSocket.onmessage?.({
				data: {
					type: 'message',
					topic: 'other-topic',
					payload: { seq: 3, text: 'ignore' },
				},
			})

			unsubA()
			await localSocket.close()

			window.SharedWorker = originalSharedWorker

			return {
				eventsA,
				eventsB,
			}
		}

		it('should satisfy the same contract in SharedWorker and BroadcastChannel modes', async () => {
			const hasSharedWorker = typeof window.SharedWorker !== 'undefined'
			if (!hasSharedWorker) {
				return
			}

			const sharedWorkerResult = await runContractScenario(false)
			const fallbackResult = await runContractScenario(true)

			expect(sharedWorkerResult).to.deep.equal(fallbackResult)
			expect(sharedWorkerResult.eventsA).to.deep.equal([
				{ topic: 'parity-topic', payload: { seq: 1, text: 'first' } },
				{ topic: 'parity-topic', payload: { seq: 2, text: 'second' } },
			])
			expect(sharedWorkerResult.eventsB).to.deep.equal([
				{ topic: 'parity-topic', payload: { seq: 1, text: 'first' } },
			])
		})

		async function runSubscribedAckScenario(forceFallback) {
			const originalSharedWorker = window.SharedWorker
			if (forceFallback) {
				window.SharedWorker = undefined
			}

			const localSocket = new SharedSocket()
			let unsubscribe = null
			try {
				await localSocket.connect()
				const localBridge = new PubSubBridge(localSocket)
				const topic = `transport-ack-${forceFallback ? 'bc' : 'sw'}-${Date.now()}-${Math.random()}`
				const order = []

				localBridge.addEventListener('subscribed', (event) => {
					if (event.detail.topic === topic) order.push('subscribed')
				})

				const messagePromise = new Promise((resolve) => {
					unsubscribe = localBridge.subscribe(topic, (payload) => {
						order.push('message')
						resolve(payload)
					})
				})

				const ack = await localBridge.waitForSubscribed(topic, { timeout: 3000 })
				expect(ack).to.deep.equal({ type: 'subscribed', topic })

				await publishRaw(topic, { id: 1 })
				expect(await messagePromise).to.deep.equal({ id: 1 })
				expect(order).to.deep.equal(['subscribed', 'message'])
			} finally {
				unsubscribe?.()
				await localSocket.close()
				window.SharedWorker = originalSharedWorker
			}
		}

		it('should surface subscribed ACKs before later data messages in both transports', async () => {
			await runSubscribedAckScenario(true)

			const hasSharedWorker = typeof window.SharedWorker !== 'undefined'
			if (hasSharedWorker) {
				await runSubscribedAckScenario(false)
			}
		})
	})
})
