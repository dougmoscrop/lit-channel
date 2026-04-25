import { expect } from '@esm-bundle/chai'
import {
	SharedSocket,
	getConfiguredAuthToken,
	getConfiguredEndpoint,
	getConfiguredWorkerUrl,
	resolveWorkerUrl,
	resolveWebSocketUrl,
} from '../../src/SharedSocket.js'
import { PubSubBridge } from '../../src/PubSubBridge.js'

describe('SharedSocket core behavior', () => {
	let OriginalSharedWorker

	beforeEach(() => {
		OriginalSharedWorker = window.SharedWorker
	})

	afterEach(() => {
		window.SharedWorker = OriginalSharedWorker
	})

	it('should return undefined when endpoint meta is missing or blank', () => {
		expect(getConfiguredEndpoint(/** @type {any} */ ({}))).to.equal(undefined)
		expect(getConfiguredWorkerUrl(/** @type {any} */ ({}))).to.equal(undefined)
		expect(getConfiguredAuthToken(/** @type {any} */ ({}))).to.equal(undefined)

		const meta = document.createElement('meta')
		meta.setAttribute('name', 'lit-channel-endpoint')
		meta.setAttribute('content', '   ')
		const workerMeta = document.createElement('meta')
		workerMeta.setAttribute('name', 'lit-channel-worker-url')
		workerMeta.setAttribute('content', '   ')
		const authMeta = document.createElement('meta')
		authMeta.setAttribute('name', 'lit-channel-auth-token')
		authMeta.setAttribute('content', '   ')
		document.head.appendChild(meta)
		document.head.appendChild(workerMeta)
		document.head.appendChild(authMeta)

		try {
			expect(getConfiguredEndpoint(document)).to.equal(undefined)
			expect(getConfiguredWorkerUrl(document)).to.equal(undefined)
			expect(getConfiguredAuthToken(document)).to.equal(undefined)
		} finally {
			meta.remove()
			workerMeta.remove()
			authMeta.remove()
		}
	})

	it('should resolve default URL and preserve ws/wss endpoints', () => {
		const locationLike = /** @type {Location} */ (/** @type {unknown} */ ({
			protocol: 'http:',
			host: 'example.test:8080',
			href: 'http://example.test:8080/app/index.html',
		}))

		expect(resolveWebSocketUrl(undefined, locationLike))
			.to.equal('ws://example.test:8080/api/ws')
		expect(resolveWebSocketUrl('ws://backend.example/ws', locationLike))
			.to.equal('ws://backend.example/ws')
		expect(resolveWebSocketUrl('wss://backend.example/ws', locationLike))
			.to.equal('wss://backend.example/ws')
	})

	it('should reject unsupported endpoint protocols', () => {
		const locationLike = /** @type {Location} */ (/** @type {unknown} */ ({
			protocol: 'https:',
			host: 'example.com',
			href: 'https://example.com/app/index.html',
		}))

		expect(() => resolveWebSocketUrl('ftp://example.com/socket', locationLike)).to.throw(
			'Invalid lit-channel endpoint protocol: ftp:'
		)
	})

	it('should resolve default worker URL and allow same-origin overrides', () => {
		const locationLike = /** @type {Location} */ (/** @type {unknown} */ ({
			protocol: 'https:',
			host: 'example.com',
			href: 'https://example.com/app/index.html',
		}))

		expect(resolveWorkerUrl(undefined, locationLike)).to.match(/\/shared-worker\.js$/)
		expect(resolveWorkerUrl('/worker/shared-worker.js', locationLike))
			.to.equal('https://example.com/worker/shared-worker.js')
		expect(resolveWorkerUrl('https://example.com/proxy/shared-worker.js', locationLike))
			.to.equal('https://example.com/proxy/shared-worker.js')
	})

	it('should reject unsupported worker URL protocols', () => {
		const locationLike = /** @type {Location} */ (/** @type {unknown} */ ({
			protocol: 'https:',
			host: 'example.com',
			href: 'https://example.com/app/index.html',
		}))

		expect(() => resolveWorkerUrl('data:text/javascript,self.onconnect=()=>{}', locationLike)).to.throw(
			'Invalid lit-channel worker URL protocol: data:'
		)
	})

	it('should pass configured worker URL to SharedWorker', async () => {
		const constructedUrls = []

		window.SharedWorker = /** @type {any} */ (class {
			constructor(url) {
				constructedUrls.push(url)
				this.port = {
					onmessage: null,
					start() {},
					postMessage() {},
					close() {},
				}
			}
		})

		const socket = new SharedSocket({ workerUrl: '/worker/shared-worker.js' })
		try {
			await socket.connect()
			expect(constructedUrls).to.deep.equal([
				`${window.location.protocol}//${window.location.host}/worker/shared-worker.js`,
			])
		} finally {
			await socket.close()
		}
	})

	it('should pass same-origin worker URL to SharedWorker without fetching', async () => {
		const constructedUrls = []
		const originalFetch = window.fetch
		let fetchCalled = false

		window.fetch = /** @type {any} */ (async () => {
			fetchCalled = true
			return {
				ok: true,
				text: async () => '',
			}
		})

		window.SharedWorker = /** @type {any} */ (class {
			constructor(url) {
				constructedUrls.push(url)
				this.port = {
					onmessage: null,
					start() {},
					postMessage() {},
					close() {},
				}
			}
		})

		const sameOriginWorkerUrl = `${window.location.protocol}//${window.location.host}/worker/same-origin.js`
		const socket = new SharedSocket({ workerUrl: sameOriginWorkerUrl })
		try {
			await socket.connect()
			expect(constructedUrls).to.deep.equal([sameOriginWorkerUrl])
			expect(fetchCalled).to.equal(false)
		} finally {
			await socket.close()
			window.fetch = originalFetch
		}
	})

	it('should inline cross-origin worker URL before constructing SharedWorker', async () => {
		const constructedUrls = []
		const originalFetch = window.fetch
		const fetchedUrls = []

		window.fetch = /** @type {any} */ (async (url) => {
			fetchedUrls.push(url)
			return {
				ok: true,
				text: async () => 'self.onconnect = () => {}',
			}
		})

		window.SharedWorker = /** @type {any} */ (class {
			constructor(url) {
				constructedUrls.push(url)
				this.port = {
					onmessage: null,
					start() {},
					postMessage() {},
					close() {},
				}
			}
		})

		const crossOriginUrl = 'https://cdn.example.com/lit-channel/shared-worker.js'
		const socket = new SharedSocket({ workerUrl: crossOriginUrl })
		try {
			await socket.connect()
			expect(fetchedUrls).to.deep.equal([crossOriginUrl])
			expect(constructedUrls).to.have.lengthOf(1)
			expect(
				constructedUrls[0].startsWith('blob:') || constructedUrls[0].startsWith('data:')
			).to.equal(true)
		} finally {
			await socket.close()
			window.fetch = originalFetch
		}
	})

	it('should fall back to broadcast channel when cross-origin worker fetch fails', async () => {
		const originalFetch = window.fetch
		const originalBroadcastChannel = window.BroadcastChannel
		const originalWebSocket = window.WebSocket
		window.fetch = /** @type {any} */ (async () => ({
			ok: false,
			status: 503,
			text: async () => '',
		}))

		let fallbackUsed = false
		const socket = new SharedSocket({ workerUrl: 'https://cdn.example.com/lit-channel/shared-worker.js' })
		try {
			window.SharedWorker = /** @type {any} */ (class {
				constructor() {
					throw new Error('SharedWorker should not be constructed when fetch fails')
				}
			})

			window.BroadcastChannel = /** @type {any} */ (class {
				constructor() {}
				postMessage() {}
				addEventListener() {}
				removeEventListener() {}
				close() { fallbackUsed = true }
			})
			window.WebSocket = /** @type {any} */ (class {
				constructor() {
					this.readyState = 1
					this.onopen = null
					this.onmessage = null
					this.onclose = null
					this.onerror = null
					setTimeout(() => this.onopen?.(), 0)
				}
				send() {}
				close() {}
			})

			await socket.connect()
			expect(socket.isReady).to.equal(true)
			await socket.close()
			expect(fallbackUsed).to.equal(true)
		} finally {
			window.fetch = originalFetch
			window.BroadcastChannel = originalBroadcastChannel
			window.WebSocket = originalWebSocket
		}
	})

	it('should de-duplicate concurrent connect calls', async () => {
		let workerConstructCount = 0
		let startCount = 0

		window.SharedWorker = /** @type {any} */ (class {
			constructor() {
				workerConstructCount += 1
				this.port = {
					onmessage: null,
					start() { startCount += 1 },
					postMessage() {},
					close() {},
				}
			}
		})

		const socket = new SharedSocket({ endpoint: '/dedupe/ws' })
		try {
			await Promise.all([socket.connect(), socket.connect(), socket.connect()])
			expect(workerConstructCount).to.equal(1)
			expect(startCount).to.equal(1)
			expect(socket.isReady).to.equal(true)
		} finally {
			await socket.close()
		}
	})

	it('should flush post-close queued messages on reconnect', async () => {
		const postedByInstance = []

		window.SharedWorker = /** @type {any} */ (class {
			constructor() {
				const posted = []
				postedByInstance.push(posted)
				this.port = {
					onmessage: null,
					start() {},
					postMessage(message) { posted.push(message) },
					close() {},
				}
			}
		})

		const socket = new SharedSocket({ endpoint: '/reconnect/ws', authToken: 'Bearer reconnect-token' })
		const queuedMessage = { type: 'publish', topic: 'reconnect-topic', payload: { value: 7 } }

		try {
			await socket.connect()
			await socket.close()
			socket.postMessage(queuedMessage)
			await socket.connect()

			expect(postedByInstance).to.have.lengthOf(2)
			expect(postedByInstance[1][0]).to.deep.equal({
				type: 'config',
				endpoint: `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/reconnect/ws`,
				authToken: 'reconnect-token',
			})
			expect(postedByInstance[1][1]).to.deep.equal(queuedMessage)
		} finally {
			await socket.close()
		}
	})

	it('should forward onmessage handler to underlying port', async () => {
		let capturedPort = null
		window.SharedWorker = /** @type {any} */ (class {
			constructor() {
				capturedPort = {
					onmessage: null,
					start() {},
					postMessage() {},
					close() {},
				}
				this.port = capturedPort
			}
		})

		const socket = new SharedSocket()
		const handler = () => {}
		try {
			await socket.connect()
			socket.onmessage = handler

			expect(socket.onmessage).to.equal(handler)
			expect(capturedPort).to.not.equal(null)
			const port = /** @type {any} */ (capturedPort)
			expect(port.onmessage).to.equal(handler)
		} finally {
			await socket.close()
		}
	})

	it('should forward resume subscribe and ack frames through SharedWorker port', async () => {
		const postedMessages = []
		let capturedPort = null

		window.SharedWorker = /** @type {any} */ (class {
			constructor() {
				capturedPort = {
					onmessage: null,
					start() {},
					postMessage(message) { postedMessages.push(message) },
					close() {},
				}
				this.port = capturedPort
			}
		})

		const socket = new SharedSocket({ endpoint: '/shared-worker-resume/ws' })
		try {
			await socket.connect()
			const bridge = new PubSubBridge(socket, {
				resumeEnabled: true,
				sessionId: 'sw-session',
				getResumeCursor(topic) {
					return topic === 'sw-resume-topic'
						? { streamSeq: 2, cursor: 'cursor-2' }
						: undefined
				},
			})

			bridge.subscribe('sw-resume-topic', () => {})

			expect(postedMessages).to.deep.include({
				type: 'subscribe',
				topic: 'sw-resume-topic',
				resume: { streamSeq: 2, cursor: 'cursor-2', sessionId: 'sw-session' },
			})

			postedMessages.length = 0
			;(/** @type {any} */ (capturedPort)).onmessage?.({
				data: {
					type: 'message',
					topic: 'sw-resume-topic',
					payload: { __rt: { streamSeq: 3, eventId: 'sw-event-3' } },
				},
			})

			expect(postedMessages).to.deep.equal([
				{ type: 'ack', topic: 'sw-resume-topic', streamSeq: 3, cursor: '3', sessionId: 'sw-session' },
			])
		} finally {
			await socket.close()
		}
	})
})
