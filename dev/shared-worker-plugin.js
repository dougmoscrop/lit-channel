import { createHash } from 'node:crypto'
import { readFile, stat } from 'node:fs/promises'
import { resolve } from 'node:path'

const hotReloadSnippetPath = resolve('dev/shared-worker-hot-reload.js')
const snippet = await readFile(hotReloadSnippetPath, 'utf-8')

/**
 * WDS plugin that:
 * 1. Computes a content-hash of shared-worker.js and injects it into
 *    SharedSocket's worker URL during dev serving.
 * 2. Watches the worker file for changes and pushes a hot-reload
 *    to every connected browser via the WDS WebSocket.
 */
export function sharedWorkerPlugin() {
	const workerPath = resolve('src/shared-worker.js')
	const workerUrlNeedle = "new URL('./shared-worker.js', import.meta.url)"
	let cachedHash = null
	let cachedMtime = 0

	async function getHash() {
		try {
			const { mtimeMs } = await stat(workerPath)
			if (mtimeMs !== cachedMtime) {
				const content = await readFile(workerPath, 'utf-8')
				cachedHash = createHash('sha256').update(content).digest('hex').slice(0, 8)
				cachedMtime = mtimeMs
				console.log('[hash-plugin] computed hash:', cachedHash)
			}
		} catch (e) {
			console.error('[hash-plugin] error reading worker file:', e.message)
			cachedHash = 'error'
		}
		return cachedHash
	}

	return {
		name: 'shared-worker',

		async serverStart({ fileWatcher, webSockets }) {
			const hash = await getHash()
			console.log('[hash-plugin] started, initial hash:', hash)

			fileWatcher.add(workerPath)
			fileWatcher.on('change', async (changed) => {
				if (resolve(changed) !== workerPath) return

				// Reset mtime so getHash() recomputes
				cachedMtime = 0
				const newHash = await getHash()
				console.log('[hash-plugin] shared-worker changed → new hash:', newHash)

				if (webSockets) {
					// Tell every connected browser to hot-reload the worker
					webSockets.sendImport(
						`data:text/javascript,import{_hotReloadWorker}from'/src/SharedSocket.js';_hotReloadWorker('${newHash}');`,
					)
					console.log('[hash-plugin] sent hot-reload to browsers')
				}
			})
		},

		async transformCacheKey(context) {
			if (context.path.endsWith('.js')) {
				return `worker-hash:${await getHash()}`
			}
		},

		async transform(context) {
			if (typeof context.body !== 'string') return

			let body = context.body
			let transformed = false

			// Inject worker hash + _hotReloadWorker into SharedSocket.js (dev-only)
			if (context.path === '/src/SharedSocket.js') {
				const hash = await getHash()
				const workerUrlWithHash = `new URL('./shared-worker.js?v=${hash}', import.meta.url)`
				if (body.includes(workerUrlNeedle)) {
					console.log('[hash-plugin] injecting worker hash →', hash, 'in', context.path)
					body = body.replaceAll(workerUrlNeedle, workerUrlWithHash)
					transformed = true
				}
				body += '\n' + snippet
				transformed = true
			}

			if (!transformed) return
			return { body, transformCache: false }
		},
	}
}
