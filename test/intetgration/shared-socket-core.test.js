import { expect } from '@esm-bundle/chai'
import {
	SharedSocket,
	getConfiguredEndpoint,
	resolveWebSocketUrl,
} from '../../src/SharedSocket.js'

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

		const meta = document.createElement('meta')
		meta.setAttribute('name', 'lit-channel-endpoint')
		meta.setAttribute('content', '   ')
		document.head.appendChild(meta)

		try {
			expect(getConfiguredEndpoint(document)).to.equal(undefined)
		} finally {
			meta.remove()
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

		const socket = new SharedSocket({ endpoint: '/reconnect/ws' })
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
})
