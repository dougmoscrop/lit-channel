import { expect } from '@esm-bundle/chai'
import { SharedSocket } from '../../src/SharedSocket.js'

describe('SharedSocket queue', () => {
	it('should queue outbound messages before connect and flush after transport initializes', async () => {
		const OriginalSharedWorker = window.SharedWorker
		const postedMessages = []

		window.SharedWorker = class {
			constructor() {
				this.port = {
					onmessage: null,
					start() {},
					postMessage(message) {
						postedMessages.push(message)
					},
					close() {},
				}
			}
		}

		const socket = new SharedSocket({ endpoint: '/queued/ws' })
		socket.postMessage({ type: 'subscribe', topic: 'before-connect' })
		socket.postMessage({ type: 'publish', topic: 'before-connect', payload: { value: 42 } })

		try {
			await socket.connect()

			expect(postedMessages[0]).to.deep.equal({
				type: 'config',
				endpoint: `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/queued/ws`,
			})
			expect(postedMessages.slice(1)).to.deep.equal([
				{ type: 'subscribe', topic: 'before-connect' },
				{ type: 'publish', topic: 'before-connect', payload: { value: 42 } },
			])
		} finally {
			window.SharedWorker = OriginalSharedWorker
			await socket.close()
		}
	})
})
