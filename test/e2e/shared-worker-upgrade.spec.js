import { test, expect } from '@playwright/test'

test.describe('SharedWorker upgrade', () => {
	test('swaps fingerprinted workers without dropping queued or in-flight messages', async ({ page }) => {
		await page.goto('/test/e2e/fixtures/blank.html')

		await page.evaluate(async () => {
			const { SharedSocket, PubSubBridge } = await import('/src/lit-channel.js')
			window.__upgradeMessages = []
			window.__upgradeEvents = []

			const socket = new SharedSocket({ workerUrl: '/test/e2e/fixtures/upgrade-worker-v1.js?version=1' })
			socket.addEventListener('upgrading', (event) => {
				window.__upgradeEvents.push({ type: 'upgrading', detail: event.detail })
			})
			socket.addEventListener('reconnected', (event) => {
				window.__upgradeEvents.push({ type: 'reconnected', detail: event.detail })
			})

			await socket.connect()
			const bridge = new PubSubBridge(socket, { resumeEnabled: true, sessionId: 'upgrade-e2e' })
			bridge.subscribe('upgrade-topic', (payload) => {
				window.__upgradeMessages.push(payload)
			})

			window.__upgradeSocket = socket
			window.__upgradeBridge = bridge
		})

		await page.evaluate(() => {
			window.__upgradeBridge.publish('upgrade-topic', { step: 'before' })
		})
		await page.waitForFunction(() => window.__upgradeMessages?.some(message => message.step === 'before'))

		await page.evaluate(() => {
			window.__upgradePromise = window.__upgradeSocket.upgradeWorker(
				'/test/e2e/fixtures/upgrade-worker-v2.js?version=2',
				{ workerVersion: '2', deadlineMs: 1000 }
			)
			window.__upgradeBridge.publish('upgrade-topic', { step: 'during' })
		})

		await page.evaluate(() => window.__upgradePromise)
		await page.evaluate(() => {
			window.__upgradeBridge.publish('upgrade-topic', { step: 'after' })
		})

		await page.waitForFunction(() => {
			const steps = (window.__upgradeMessages || []).map(message => message.step)
			return ['before', 'old-inflight', 'during', 'after'].every(step => steps.includes(step))
		})

		const result = await page.evaluate(() => ({
			messages: window.__upgradeMessages.map(message => ({
				step: message.step,
				workerVersion: message.workerVersion,
				streamSeq: message.__rt?.streamSeq,
			})),
			events: window.__upgradeEvents.map(event => ({
				type: event.type,
				workerVersion: event.detail?.workerVersion,
			})),
		}))

		expect(result.messages.map(message => message.step)).toEqual([
			'before',
			'old-inflight',
			'during',
			'after',
		])
		expect(result.messages.map(message => message.workerVersion)).toEqual(['1', '1', '2', '2'])
		expect(result.messages.map(message => message.streamSeq)).toEqual([101, 102, 201, 202])
		expect(result.events.map(event => event.type)).toEqual(['upgrading', 'reconnected'])
		expect(result.events[1].workerVersion).toBe('2')
	})
})
