// --- injected by hash-plugin (dev only) ---
// This function is appended to SharedSocket.js at serve-time.
// It has access to SharedSocket.js module-scoped variables:
//   sharedWorkerUrl
export function _hotReloadWorker(newHash) {
	if (!('SharedWorker' in window)) return
	const nextWorkerUrl = new URL(sharedWorkerUrl.toString(), window.location.href)
	nextWorkerUrl.searchParams.set('v', newHash)

	console.debug('[SharedSocket] hot-reloading shared worker, hash:', newHash)
	return reloadSharedWorkers(nextWorkerUrl.toString(), {
		workerVersion: newHash,
		deadlineMs: 2000,
	})
}
