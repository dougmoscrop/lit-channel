export async function waitForMessageCount(page, key, count, timeout = 3000) {
	await page.waitForFunction(
		([k, c]) => Array.isArray(window[k]) && window[k].length >= c,
		[key, count],
		{ timeout },
	)
}

export async function initMessageCollector(page, key = '__messages') {
	await page.evaluate((collectorKey) => {
		window[collectorKey] = []
		document.addEventListener('lit-channel-message', (e) => {
			window[collectorKey].push(e.detail)
		})
	}, key)
}

export async function forceBroadcastFallback(context) {
	await context.addInitScript(() => {
		window.SharedWorker = undefined
	})
}

export async function createContexts(browser, count) {
	const contexts = []
	const pages = []
	for (let i = 0; i < count; i++) {
		const context = await browser.newContext()
		const page = await context.newPage()
		contexts.push(context)
		pages.push(page)
	}
	return { contexts, pages }
}

export async function closeContexts(contexts = [], pages = []) {
	for (const page of pages) {
		try {
			if (!page?.isClosed?.()) {
				await page.close()
			}
		} catch (_) {}
	}

	for (const context of contexts) {
		try {
			if (!context?.isBeingClosed?.()) {
				await context.close()
			}
		} catch (_) {}
	}
}

export function buildSequencedPayload(text, sequence, extra = {}) {
	return {
		text,
		ts: Date.now(),
		sequence,
		...extra,
	}
}
