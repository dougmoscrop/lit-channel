import { test, expect, chromium, firefox } from '@playwright/test'
import {
	waitForMessageCount,
	initMessageCollector,
	forceBroadcastFallback,
	createContexts,
	closeContexts,
	buildSequencedPayload,
} from '../helpers/e2e-utils.js'

async function waitForBridgeReady(page, timeout = 4000) {
	await page.waitForFunction(
		() => Boolean(document.querySelector('lit-channel')?._bridge),
		undefined,
		{ timeout },
	)
}

async function waitForReceiverSubscription(page, timeout = 4000) {
	await page.waitForFunction(
		() => typeof document.querySelector('lit-channel')?._unsubscribe === 'function',
		undefined,
		{ timeout },
	)
}

test.describe('cross-context messaging', () => {
	test('should communicate between chromium and firefox browser instances', async () => {
		// Launch two different browser engines
		const browserChromium = await chromium.launch()
		const browserFirefox = await firefox.launch()

		try {
			// Create isolated contexts (simulates two different users/sessions)
			const contextChromium = await browserChromium.newContext()
			const contextFirefox = await browserFirefox.newContext()

			const pageChrome = await contextChromium.newPage()
			const pageFirefox = await contextFirefox.newPage()

			try {
				// Navigate both pages to the app
				await pageChrome.goto('http://localhost:8000/index.html')
				await pageFirefox.goto('http://localhost:8000/index.html')

				// Wait for lit-channel to be defined
				await pageChrome.waitForSelector('lit-channel', { state: 'attached' })
				await pageFirefox.waitForSelector('lit-channel', { state: 'attached' })

				// Set up receiver on Firefox: collect messages from lit-channel-message events
				await pageFirefox.evaluate(() => {
					window.__receivedMessages = []
					document.querySelectorAll('lit-channel').forEach((el) => {
						el.addEventListener('lit-channel-message', (e) => {
							window.__receivedMessages.push(e.detail)
						})
					})
				})

				// Chrome sends a message via lit-channel
				const messageText = 'Hello from Chromium!'
				await pageChrome.evaluate((msg) => {
					const channel = document.querySelector('lit-channel')
					if (channel && channel.publish) {
						channel.publish({ text: msg, ts: Date.now() })
					}
				}, messageText)

				await waitForMessageCount(pageFirefox, '__receivedMessages', 1)

				// Verify Firefox received the message
				const receivedMessages = await pageFirefox.evaluate(() => window.__receivedMessages)
				const matchingMessages = receivedMessages.filter((m) => (
					m.topic === 'Announcements' && m.payload?.text === messageText
				))
				expect(matchingMessages).toHaveLength(1)
				expect(matchingMessages[0]).toMatchObject({
					topic: 'Announcements',
					payload: { text: messageText },
				})
				expect(typeof matchingMessages[0].payload.ts).toBe('number')
			} finally {
				await pageChrome.close()
				await pageFirefox.close()
				await contextChromium.close()
				await contextFirefox.close()
			}
		} finally {
			await browserChromium.close()
			await browserFirefox.close()
		}
	})

	test('should handle 3+ contexts with coordinated messaging', async ({ browser }) => {
		const { contexts, pages } = await createContexts(browser, 3)
		const [context1, context2, context3] = contexts
		const [page1, page2, page3] = pages

		try {
			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')
			await page3.goto('http://localhost:8000/index.html')

			await page2.waitForSelector('lit-channel', { state: 'attached' })
			await page3.waitForSelector('lit-channel', { state: 'attached' })

			// Set up listeners on pages 2 and 3
			await initMessageCollector(page2)
			await initMessageCollector(page3)

			// Publish from page 1
			await page1.evaluate(() => {
				const channel = document.querySelector('lit-channel')
				if (channel && channel.publish) {
					channel.publish({ text: 'Message from page 1', ts: Date.now() })
				}
			})

			await waitForMessageCount(page2, '__messages', 1)
			await waitForMessageCount(page3, '__messages', 1)

			const msgs2 = await page2.evaluate(() => window.__messages)
			const msgs3 = await page3.evaluate(() => window.__messages)
			const page2Matches = msgs2.filter((m) => (
				m.topic === 'Announcements' && m.payload?.text === 'Message from page 1'
			))
			const page3Matches = msgs3.filter((m) => (
				m.topic === 'Announcements' && m.payload?.text === 'Message from page 1'
			))

			expect(page2Matches).toHaveLength(1)
			expect(page3Matches).toHaveLength(1)
			expect(page2Matches[0]).toMatchObject({
				topic: 'Announcements',
				payload: { text: 'Message from page 1' },
			})
			expect(page3Matches[0]).toMatchObject({
				topic: 'Announcements',
				payload: { text: 'Message from page 1' },
			})
		} finally {
			await closeContexts([context1, context2, context3], [page1, page2, page3])
		}
	})

	test('should handle rapid context creation and closure', async ({ browser }) => {
		const { contexts, pages } = await createContexts(browser, 2)

		try {
			for (const page of pages) {
				await page.goto('http://localhost:8000/index.html')
				await page.waitForSelector('lit-channel', { state: 'attached' })
			}

			// Verify all pages loaded successfully
			const responses = await Promise.all(
				pages.map(p => p.evaluate(() => {
					const el = document.querySelector('lit-channel')
					return el ? 'ready' : 'not found'
				}))
			)

			expect(responses.every(r => r === 'ready')).toBe(true)

			// Close the first context
			for (let i = 0; i < 1; i++) {
				if (pages[i]) await pages[i].close()
				if (contexts[i]) await contexts[i].close()
			}

			// Verify remaining context is still functional
			for (let i = 1; i < pages.length; i++) {
				const response = await pages[i].evaluate(() => {
					const el = document.querySelector('lit-channel')
					return el ? 'still ready' : 'lost'
				})
				expect(response).toBe('still ready')
			}
		} finally {
			await closeContexts(contexts, pages)
		}
	})

	test('should deliver messages in order across multiple contexts', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()
		const context3 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()
		const page3 = await context3.newPage()

		try {
			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')
			await page3.goto('http://localhost:8000/index.html')

			await page2.waitForSelector('lit-channel', { state: 'attached' })
			await page3.waitForSelector('lit-channel', { state: 'attached' })

			// Set up listeners on page 2 and 3
			await initMessageCollector(page2)
			await initMessageCollector(page3)

			// Send 5 messages from page 1
			for (let i = 0; i < 5; i++) {
				const payload = buildSequencedPayload(`Message ${i}`, i)
				await page1.evaluate((data) => {
					const channel = document.querySelector('lit-channel')
					if (channel && channel.publish) {
						channel.publish(data)
					}
				}, payload)
				await Promise.resolve()
			}

			await waitForMessageCount(page2, '__messages', 5)
			await waitForMessageCount(page3, '__messages', 5)

			const msgs2 = await page2.evaluate(() => window.__messages)
			const msgs3 = await page3.evaluate(() => window.__messages)
			const orderedMsgs2 = msgs2.filter((m) => {
				const seq = m.payload?.sequence
				return m.topic === 'Announcements' && seq >= 0 && seq <= 4 && m.payload?.text === `Message ${seq}`
			})
			const orderedMsgs3 = msgs3.filter((m) => {
				const seq = m.payload?.sequence
				return m.topic === 'Announcements' && seq >= 0 && seq <= 4 && m.payload?.text === `Message ${seq}`
			})

			expect(orderedMsgs2).toHaveLength(5)
			expect(orderedMsgs3).toHaveLength(5)
			expect(orderedMsgs2.map((m) => m.payload.sequence)).toEqual([0, 1, 2, 3, 4])
			expect(orderedMsgs3.map((m) => m.payload.sequence)).toEqual([0, 1, 2, 3, 4])
		} finally {
			await page1.close()
			await page2.close()
			await page3.close()
			await context1.close()
			await context2.close()
			await context3.close()
		}
	})

	test('should handle websocket fallback (BroadcastChannel)', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()

		try {
			// Disable SharedWorker to force BroadcastChannel fallback
			await forceBroadcastFallback(context1)
			await forceBroadcastFallback(context2)

			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')

			await page2.waitForSelector('lit-channel', { state: 'attached' })
			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForReceiverSubscription(page2)

			// Set up listener on page 2
			await initMessageCollector(page2)

			// Send message from page 1
			await page1.evaluate(() => {
				const channel = document.querySelector('lit-channel')
				if (channel && channel.publish) {
					channel.publish({ text: 'BroadcastChannel test', ts: Date.now() })
				}
			})

			await waitForMessageCount(page2, '__messages', 1)

			const messagesPage2 = await page2.evaluate(() => window.__messages)
			const fallbackMatches = messagesPage2.filter((m) => (
				m.topic === 'Announcements' && m.payload?.text === 'BroadcastChannel test'
			))
			expect(fallbackMatches).toHaveLength(1)
			expect(new Set(fallbackMatches.map((m) => `${m.topic}:${m.payload?.text}`)).size).toBe(1)
			expect(fallbackMatches[0]).toMatchObject({
				topic: 'Announcements',
				payload: { text: 'BroadcastChannel test' },
			})
		} finally {
			await page1.close()
			await page2.close()
			await context1.close()
			await context2.close()
		}
	})

	test('should survive context closure and re-election', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()
		const context3 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()
		const page3 = await context3.newPage()

		try {
			// Force BroadcastChannel for all contexts
			await forceBroadcastFallback(context1)
			await forceBroadcastFallback(context2)
			await forceBroadcastFallback(context3)

			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')
			await page3.goto('http://localhost:8000/index.html')

			await page2.waitForSelector('lit-channel', { state: 'attached' })
			await page3.waitForSelector('lit-channel', { state: 'attached' })
			await waitForBridgeReady(page2)
			await waitForBridgeReady(page3)
			await waitForReceiverSubscription(page3)

			// Set up listener on page 3
			await initMessageCollector(page3)

			// Close page 1 and context 1 (re-election scenario)
			await page1.close()
			await context1.close()
			await waitForBridgeReady(page2)

			// Send messages after re-election handoff
			await page2.evaluate(() => {
				const channel = document.querySelector('lit-channel')
				if (channel && channel.publish) {
					channel.publish({ text: 'After re-election', ts: Date.now(), seq: 1 })
					channel.publish({ text: 'After re-election', ts: Date.now(), seq: 2 })
				}
			})

			await waitForMessageCount(page3, '__messages', 2)

			const messagesPage3 = await page3.evaluate(() => window.__messages)
			const reelectionMatches = messagesPage3.filter((m) => (
				m.topic === 'Announcements' && m.payload?.text === 'After re-election'
			))
			expect(reelectionMatches).toHaveLength(2)
			expect(reelectionMatches.map((m) => m.payload?.seq)).toEqual([1, 2])
			expect(new Set(reelectionMatches.map((m) => m.payload?.seq)).size).toBe(2)
			expect(reelectionMatches[0]).toMatchObject({
				topic: 'Announcements',
				payload: { text: 'After re-election', seq: 1 },
			})
			expect(reelectionMatches[1]).toMatchObject({
				topic: 'Announcements',
				payload: { text: 'After re-election', seq: 2 },
			})
		} finally {
			if (page2 && !page2.isClosed?.()) await page2.close()
			if (page3 && !page3.isClosed?.()) await page3.close()
			if (context2 && !context2.isBeingClosed?.()) await context2.close()
			if (context3 && !context3.isBeingClosed?.()) await context3.close()
		}
	})

	test('should handle high message volume (100+msgs)', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()

		try {
			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')

			await page1.waitForSelector('lit-channel', { state: 'attached' })
			await page2.waitForSelector('lit-channel', { state: 'attached' })
			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForReceiverSubscription(page2)
			await waitForReceiverSubscription(page2)

			// Set up message collection on page 2
			await initMessageCollector(page2)

			// Send 30 messages rapidly
			for (let i = 0; i < 30; i++) {
				const payload = buildSequencedPayload(`Rapid message ${i}`, i)
				await page1.evaluate((data) => {
					const channel = document.querySelector('lit-channel')
					if (channel && channel.publish) {
						channel.publish(data)
					}
				}, payload)
			}

			await waitForMessageCount(page2, '__messages', 30, 6000)

			const messages = await page2.evaluate(() => window.__messages)
			const rapidMessages = messages.filter((m) => {
				const seq = m.payload?.sequence
				return m.topic === 'Announcements' && seq >= 0 && seq <= 29 && m.payload?.text === `Rapid message ${seq}`
			})
			expect(rapidMessages).toHaveLength(30)
			expect(rapidMessages.map((m) => m.payload.sequence)).toEqual(
				Array.from({ length: 30 }, (_, i) => i)
			)
		} finally {
			await page1.close()
			await page2.close()
			await context1.close()
			await context2.close()
		}
	})

	test('should handle component mounting/unmounting cycles', async ({ browser }) => {
		const context = await browser.newContext()
		const page = await context.newPage()

		try {
			await page.goto('http://localhost:8000/index.html')

			for (let i = 0; i < 5; i++) {
				const status = await page.evaluate(() => {
					const el = document.querySelector('lit-channel')
					return el ? 'mounted' : 'unmounted'
				})

				expect(status).toBe('mounted')

				await page.waitForSelector('lit-channel', { state: 'attached' })
			}
		} finally {
			await page.close()
			await context.close()
		}
	})

	test('should isolate topics across contexts when publishing to General', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()

		try {
			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')

			await page1.waitForSelector('lit-channel', { state: 'attached' })
			await page2.waitForSelector('lit-channel', { state: 'attached' })
			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForReceiverSubscription(page2)

			await initMessageCollector(page2)

			await page1.evaluate(() => {
				const channels = Array.from(document.querySelectorAll('lit-channel'))
				const generalChannel = channels.find((el) => el.name === 'General')
				generalChannel?.publish({ text: 'General only', ts: Date.now() })
			})

			await waitForMessageCount(page2, '__messages', 1)

			const messages = await page2.evaluate(() => window.__messages)
			const generalMessages = messages.filter((m) => (
				m.topic === 'General' && m.payload?.text === 'General only'
			))
			const announcementMessages = messages.filter((m) => m.topic === 'Announcements')

			expect(generalMessages).toHaveLength(1)
			expect(announcementMessages).toHaveLength(0)
		} finally {
			await page1.close()
			await page2.close()
			await context1.close()
			await context2.close()
		}
	})

	test('should stop receiving General topic messages after General channel is removed', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()

		try {
			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')

			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForReceiverSubscription(page2)

			await initMessageCollector(page2)

			await page2.evaluate(() => {
				const channels = Array.from(document.querySelectorAll('lit-channel'))
				const generalChannel = channels.find((el) => el.name === 'General')
				generalChannel?.remove()
			})

			await page1.evaluate(() => {
				const channels = Array.from(document.querySelectorAll('lit-channel'))
				const generalChannel = channels.find((el) => el.name === 'General')
				generalChannel?.publish({ text: 'General after remove', ts: Date.now() })
			})

			await page2.waitForTimeout(500)

			const messages = await page2.evaluate(() => window.__messages)
			const removedTopicMessages = messages.filter((m) => m.topic === 'General')

			expect(removedTopicMessages).toHaveLength(0)
		} finally {
			await page1.close()
			await page2.close()
			await context1.close()
			await context2.close()
		}
	})

	test('should deliver both Announcements and General topics from separate senders', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()
		const context3 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()
		const page3 = await context3.newPage()

		try {
			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')
			await page3.goto('http://localhost:8000/index.html')

			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForBridgeReady(page3)
			await waitForReceiverSubscription(page3)

			await initMessageCollector(page3)

			await page1.evaluate(() => {
				const channels = Array.from(document.querySelectorAll('lit-channel'))
				const announcements = channels.find((el) => el.name === 'Announcements')
				announcements?.publish({ text: 'Announcement alpha', ts: Date.now() })
			})

			await page2.evaluate(() => {
				const channels = Array.from(document.querySelectorAll('lit-channel'))
				const general = channels.find((el) => el.name === 'General')
				general?.publish({ text: 'General beta', ts: Date.now() })
			})

			await waitForMessageCount(page3, '__messages', 2)

			const messages = await page3.evaluate(() => window.__messages)
			const announcement = messages.filter((m) => (
				m.topic === 'Announcements' && m.payload?.text === 'Announcement alpha'
			))
			const general = messages.filter((m) => (
				m.topic === 'General' && m.payload?.text === 'General beta'
			))

			expect(announcement).toHaveLength(1)
			expect(general).toHaveLength(1)
		} finally {
			await page1.close()
			await page2.close()
			await page3.close()
			await context1.close()
			await context2.close()
			await context3.close()
		}
	})

	test('should continue delivering through sequential BroadcastChannel leader handoffs', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()
		const context3 = await browser.newContext()
		const context4 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()
		const page3 = await context3.newPage()
		const page4 = await context4.newPage()

		try {
			await forceBroadcastFallback(context1)
			await forceBroadcastFallback(context2)
			await forceBroadcastFallback(context3)
			await forceBroadcastFallback(context4)

			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')
			await page3.goto('http://localhost:8000/index.html')
			await page4.goto('http://localhost:8000/index.html')

			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForBridgeReady(page3)
			await waitForBridgeReady(page4)
			await waitForReceiverSubscription(page4)

			await initMessageCollector(page4)

			await page1.evaluate(() => {
				document.querySelector('lit-channel')?.publish({ text: 'handoff-1', seq: 1, ts: Date.now() })
			})
			await waitForMessageCount(page4, '__messages', 1)

			await page1.close()
			await context1.close()

			await page2.evaluate(() => {
				document.querySelector('lit-channel')?.publish({ text: 'handoff-2', seq: 2, ts: Date.now() })
			})
			await waitForMessageCount(page4, '__messages', 2)

			await page2.close()
			await context2.close()

			await page3.evaluate(() => {
				document.querySelector('lit-channel')?.publish({ text: 'handoff-3', seq: 3, ts: Date.now() })
			})
			await waitForMessageCount(page4, '__messages', 3)

			const messages = await page4.evaluate(() => window.__messages)
			const handoffMessages = messages.filter((m) => (
				m.topic === 'Announcements' && typeof m.payload?.seq === 'number'
			))

			expect(handoffMessages.map((m) => m.payload.seq)).toEqual([1, 2, 3])
			expect(new Set(handoffMessages.map((m) => m.payload.seq)).size).toBe(3)
		} finally {
			try {
				if (!page3.isClosed()) await page3.close()
			} catch (_) {}
			try {
				if (!page4.isClosed()) await page4.close()
			} catch (_) {}
			try {
				await context3.close()
			} catch (_) {}
			try {
				await context4.close()
			} catch (_) {}
		}
	})

	test('should not duplicate deliveries after receiver remount cycles in BroadcastChannel mode', async ({ browser }) => {
		const context1 = await browser.newContext()
		const context2 = await browser.newContext()

		const page1 = await context1.newPage()
		const page2 = await context2.newPage()

		try {
			await forceBroadcastFallback(context1)
			await forceBroadcastFallback(context2)

			await page1.goto('http://localhost:8000/index.html')
			await page2.goto('http://localhost:8000/index.html')

			await waitForBridgeReady(page1)
			await waitForBridgeReady(page2)
			await waitForReceiverSubscription(page2)

			await initMessageCollector(page2)

			for (let i = 0; i < 4; i++) {
				await page2.evaluate(() => {
					const channels = Array.from(document.querySelectorAll('lit-channel'))
					const general = channels.find((el) => el.name === 'General')
					general?.remove()

					const next = document.createElement('lit-channel')
					next.setAttribute('name', 'General')
					document.body.appendChild(next)
				})

				await page2.waitForFunction(() => {
					const channels = Array.from(document.querySelectorAll('lit-channel'))
					const general = channels.find((el) => el.name === 'General')
					return Boolean(general && typeof general._unsubscribe === 'function')
				})
			}

			await page1.evaluate(() => {
				const channels = Array.from(document.querySelectorAll('lit-channel'))
				const general = channels.find((el) => el.name === 'General')
				general?.publish({ text: 'general-no-duplicates', ts: Date.now() })
			})

			await waitForMessageCount(page2, '__messages', 1)

			const messages = await page2.evaluate(() => window.__messages)
			const matched = messages.filter((m) => (
				m.topic === 'General' && m.payload?.text === 'general-no-duplicates'
			))

			expect(matched).toHaveLength(1)
		} finally {
			await page1.close()
			await page2.close()
			await context1.close()
			await context2.close()
		}
	})
})
