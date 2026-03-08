import { expect } from '@esm-bundle/chai'
import { SharedSocket } from '../../src/SharedSocket.js'
import { PubSubBridge } from '../../src/PubSubBridge.js'

/**
 * Unit tests for PubSubBridge - testing pub/sub behavior, edge cases, and resilience
 */
describe('PubSubBridge', () => {
	const subscriptions = []
	let socket = null
	let bridge = null

	beforeEach(async () => {
		socket = new SharedSocket()
		await socket.connect()
		bridge = new PubSubBridge(socket)
	})

	afterEach(() => {
		subscriptions.forEach(unsub => unsub?.())
		subscriptions.length = 0
	})

	describe('core functionality', () => {
		it('should initialize with valid socket', () => {
			expect(socket).to.exist
			expect(socket.isReady).to.be.true
		})

		it('should call socket.postMessage on subscribe', () => {
			let postCalled = false
			const originalPostMessage = socket.postMessage
			socket.postMessage = (msg) => {
				postCalled = true
				expect(msg).to.deep.equal({ type: 'subscribe', topic: 'test-topic' })
			}

			const unsub = bridge.subscribe('test-topic', () => {})
			subscriptions.push(unsub)
			expect(postCalled).to.be.true
			socket.postMessage = originalPostMessage
		})

		it('should return unsubscribe function', () => {
			const unsub = bridge.subscribe('test-topic', () => {})
			subscriptions.push(unsub)
			expect(unsub).to.be.a('function')
		})

		it('should deliver messages with payload and topic', (done) => {
			const callback = (payload, topic) => {
				expect(payload).to.deep.equal({ text: 'hello' })
				expect(topic).to.equal('test-topic')
				done()
			}
			const unsub = bridge.subscribe('test-topic', callback)
			subscriptions.push(unsub)

			setTimeout(() => {
				socket.onmessage?.({
					data: {
						type: 'message',
						topic: 'test-topic',
						payload: { text: 'hello' },
					},
				})
			}, 10)
		})

		it('should send unsubscribe message to socket', () => {
			let unsubscribed = false
			const originalPostMessage = socket.postMessage
			socket.postMessage = (msg) => {
				if (msg.type === 'unsubscribe' && msg.topic === 'test-topic') {
					unsubscribed = true
				}
			}

			const unsub = bridge.subscribe('test-topic', () => {})
			subscriptions.push(unsub)
			unsub()
			expect(unsubscribed).to.be.true
			socket.postMessage = originalPostMessage
		})

		it('should de-duplicate duplicate subscribe callback behavior per topic', async () => {
			let invokeCount = 0
			const callback = () => {
				invokeCount += 1
			}

			const unsubA = bridge.subscribe('dup-callback-topic', callback)
			const unsubB = bridge.subscribe('dup-callback-topic', callback)
			subscriptions.push(unsubA, unsubB)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'dup-callback-topic',
					payload: { msg: 'once' },
				},
			})

			await Promise.resolve()
			expect(invokeCount).to.equal(1)
		})

		it('should treat returned unsubscribe as idempotent', async () => {
			const unsubscribeMessages = []
			const originalPostMessage = socket.postMessage
			socket.postMessage = (msg) => {
				if (msg.type === 'unsubscribe' && msg.topic === 'idempotent-unsub-topic') {
					unsubscribeMessages.push(msg)
				}
				originalPostMessage.call(socket, msg)
			}

			const unsub = bridge.subscribe('idempotent-unsub-topic', () => {})
			subscriptions.push(unsub)

			unsub()
			unsub()

			await Promise.resolve()
			expect(unsubscribeMessages).to.have.lengthOf(1)

			socket.postMessage = originalPostMessage
		})
	})

	describe('edge cases: topic names', () => {
		it('should handle topics with special characters', async () => {
			const results = []
			const unsub = bridge.subscribe('topic:with-special.chars_123', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'topic:with-special.chars_123',
					payload: { msg: 'test' },
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
		})

		it('should handle very long topic names (1KB+)', async () => {
			const results = []
			const longTopic = 'a'.repeat(1024)

			const unsub = bridge.subscribe(longTopic, (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: longTopic,
					payload: { msg: 'test' },
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
		})

		it('should handle unicode topic names', async () => {
			const results = []
			const unicodeTopic = 'topic-🚀-中文-العربية'

			const unsub = bridge.subscribe(unicodeTopic, (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: unicodeTopic,
					payload: { msg: 'test' },
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
		})
	})

	describe('edge cases: payload handling', () => {
		it('should handle null payload', async () => {
			const results = []
			const unsub = bridge.subscribe('null-payload-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'null-payload-topic',
					payload: null,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
			expect(results[0]).to.equal(null)
		})

		it('should handle undefined payload', async () => {
			const results = []
			const unsub = bridge.subscribe('undefined-payload-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'undefined-payload-topic',
					payload: undefined,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
		})

		it('should handle very large payloads (1MB+)', async () => {
			const results = []
			const largePayload = { data: 'x'.repeat(1024 * 1024) }

			const unsub = bridge.subscribe('large-payload-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'large-payload-topic',
					payload: largePayload,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
			expect(results[0].data).to.have.lengthOf(1024 * 1024)
		})

		it('should handle complex nested payloads', async () => {
			const results = []
			const complexPayload = {
				level1: {
					level2: {
						level3: {
							array: [1, 2, 3, { nested: true }],
							bool: true,
							str: 'nested',
						},
					},
				},
			}

			const unsub = bridge.subscribe('complex-payload-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'complex-payload-topic',
					payload: complexPayload,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results).to.have.lengthOf(1)
			expect(results[0]).to.deep.equal(complexPayload)
		})

		it('should handle numeric payloads', async () => {
			const results = []
			const unsub = bridge.subscribe('numeric-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'numeric-topic',
					payload: 42,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results[0]).to.equal(42)
		})

		it('should handle boolean payloads', async () => {
			const results = []
			const unsub = bridge.subscribe('bool-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'bool-topic',
					payload: true,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results[0]).to.equal(true)
		})

		it('should handle string payloads with special characters', async () => {
			const results = []
			const specialString = '🎉 "quotes" \'apostrophes\' \n newlines \t tabs \\ backslash'

			const unsub = bridge.subscribe('special-string-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'special-string-topic',
					payload: specialString,
				},
			})
			await new Promise(r => setTimeout(r, 10))
			expect(results[0]).to.equal(specialString)
		})
	})

	describe('stress testing', () => {
		it('should handle rapid successive subscribes and unsubscribes', async () => {
			const topic = 'rapid-test'
			let unsubCount = 0
			const original = socket.postMessage
			socket.postMessage = (msg) => {
				if (msg.type === 'unsubscribe') unsubCount++
			}

			for (let i = 0; i < 10; i++) {
				const unsub = bridge.subscribe(topic, () => {})
				subscriptions.push(unsub)
				unsub()
			}

			await new Promise(r => setTimeout(r, 50))
			expect(unsubCount).to.equal(10)
			socket.postMessage = original
		})

		it('should handle many simultaneous subscriptions (50+)', async () => {
			const results = []

			for (let i = 0; i < 50; i++) {
				const unsub = bridge.subscribe('many-subs-topic', (payload) => {
					results.push(i)
				})
				subscriptions.push(unsub)
			}

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'many-subs-topic',
					payload: { msg: 'test' },
				},
			})

			await new Promise(r => setTimeout(r, 20))
			expect(results).to.have.lengthOf(50)
		})

		it('should handle rapid message arrival (100 msgs)', async () => {
			const results = []
			const unsub = bridge.subscribe('rapid-delivery-topic', (payload) => {
				results.push(payload.seq)
			})
			subscriptions.push(unsub)

			for (let i = 0; i < 100; i++) {
				socket.onmessage?.({
					data: {
						type: 'message',
						topic: 'rapid-delivery-topic',
						payload: { seq: i },
					},
				})
			}

			await new Promise(r => setTimeout(r, 50))
			expect(results).to.have.lengthOf(100)
			expect(results[0]).to.equal(0)
			expect(results[99]).to.equal(99)
		})

		it('should handle interleaved subscribes and message arrivals', async () => {
			const results = []

			for (let i = 0; i < 10; i++) {
				const unsub = bridge.subscribe(`interleave-topic-${i}`, (payload) => {
					results.push({ topic: i, seq: payload.seq })
				})
				subscriptions.push(unsub)

				socket.onmessage?.({
					data: {
						type: 'message',
						topic: `interleave-topic-${i}`,
						payload: { seq: i },
					},
				})
			}

			await new Promise(r => setTimeout(r, 50))
			expect(results).to.have.lengthOf(10)
		})
	})

	describe('message ordering & delivery', () => {
		it('should preserve message delivery order', async () => {
			const results = []
			const unsub = bridge.subscribe('order-test', (payload) => {
				results.push(payload.order)
			})
			subscriptions.push(unsub)

			for (let i = 0; i < 20; i++) {
				socket.onmessage?.({
					data: {
						type: 'message',
						topic: 'order-test',
						payload: { order: i },
					},
				})
			}

			await new Promise(r => setTimeout(r, 50))
			for (let i = 0; i < 20; i++) {
				expect(results[i]).to.equal(i)
			}
		})

		it('should handle unsubscribe during message delivery', async () => {
			const results = []
			const unsub = bridge.subscribe('dynamic-unsub-topic', (payload) => {
				results.push(payload.seq)
				if (payload.seq === 2) {
					unsub()
				}
			})
			subscriptions.push(unsub)

			for (let i = 0; i < 10; i++) {
				socket.onmessage?.({
					data: {
						type: 'message',
						topic: 'dynamic-unsub-topic',
						payload: { seq: i },
					},
				})
			}

			await new Promise(r => setTimeout(r, 50))
			expect(results).to.have.lengthOf(3)
			expect(results).to.deep.equal([0, 1, 2])
		})

		it('should deliver to subscriber added mid-stream', async () => {
			const results1 = []
			const results2 = []

			const unsub1 = bridge.subscribe('mid-stream-topic', (payload) => {
				results1.push(payload.seq)
			})
			subscriptions.push(unsub1)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'mid-stream-topic',
					payload: { seq: 0 },
				},
			})

			const unsub2 = bridge.subscribe('mid-stream-topic', (payload) => {
				results2.push(payload.seq)
			})
			subscriptions.push(unsub2)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'mid-stream-topic',
					payload: { seq: 1 },
				},
			})

			await new Promise(r => setTimeout(r, 30))
			expect(results1).to.have.lengthOf(2)
			expect(results2).to.have.lengthOf(1)
		})
	})

	describe('callback resilience', () => {
		it('should continue delivering to other subscribers if one throws', async () => {
			const results = []

			const unsub1 = bridge.subscribe('error-resilience-topic', () => {
				try {
					throw new Error('callback error')
				} catch (e) {
					// swallow error
				}
			})
			subscriptions.push(unsub1)

			const unsub2 = bridge.subscribe('error-resilience-topic', (payload) => {
				results.push(payload)
			})
			subscriptions.push(unsub2)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'error-resilience-topic',
					payload: { msg: 'should reach subscriber 2' },
				},
			})

			await new Promise(r => setTimeout(r, 20))
			expect(results).to.have.lengthOf(1)
			expect(results[0]).to.deep.equal({ msg: 'should reach subscriber 2' })
		})

		it('should handle subscriber that throws synchronously', async () => {
			let errorThrown = false

			const unsub = bridge.subscribe('sync-error-topic', () => {
				errorThrown = true
				try {
					throw new Error('sync error')
				} catch (e) {
					// expected
				}
			})
			subscriptions.push(unsub)

			socket.onmessage?.({
				data: {
					type: 'message',
					topic: 'sync-error-topic',
					payload: { msg: 'test' },
				},
			})

			await new Promise(r => setTimeout(r, 10))
			expect(errorThrown).to.be.true
		})
	})

	describe('cleanup & memory', () => {
		it('should not leak references after unsubscribe', async () => {
			const before = Object.keys(window).length

			for (let i = 0; i < 10; i++) {
				const unsub = bridge.subscribe(`cleanup-test-${i}`, () => {})
				unsub()
			}

			await new Promise(r => setTimeout(r, 30))
			const after = Object.keys(window).length

			expect(Math.abs(after - before)).to.be.lessThan(5)
		})
	})

	describe('negative-path handling', () => {
		it('should ignore malformed inbound envelopes', async () => {
			const received = []
			const unsub = bridge.subscribe('valid-topic', (payload) => {
				received.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({ data: { type: 'ping' } })
			socket.onmessage?.({ data: { type: 'message' } })
			socket.onmessage?.({ data: { type: 'message', payload: { bad: true } } })
			socket.onmessage?.({ data: { type: 'unknown', topic: 'valid-topic', payload: { bad: true } } })

			await Promise.resolve()
			expect(received).to.have.lengthOf(0)
		})

		it('should ignore inbound messages with missing payload for other topics', async () => {
			const received = []
			const unsub = bridge.subscribe('topic-a', (payload) => {
				received.push(payload)
			})
			subscriptions.push(unsub)

			socket.onmessage?.({ data: { type: 'message', topic: 'topic-b' } })
			await Promise.resolve()
			expect(received).to.have.lengthOf(0)
		})

		it('should handle invalid publish calls without throwing', () => {
			const posted = []
			const originalPostMessage = socket.postMessage
			socket.postMessage = (msg) => {
				posted.push(msg)
			}

			expect(() => bridge.publish(undefined, undefined)).to.not.throw()
			expect(() => bridge.publish('', { bad: true })).to.not.throw()
			expect(() => bridge.publish(null, { bad: 'topic' })).to.not.throw()

			expect(posted).to.deep.equal([
				{ type: 'publish', topic: undefined, payload: undefined },
				{ type: 'publish', topic: '', payload: { bad: true } },
				{ type: 'publish', topic: null, payload: { bad: 'topic' } },
			])

			socket.postMessage = originalPostMessage
		})
	})
})
