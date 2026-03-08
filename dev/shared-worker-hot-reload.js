// --- injected by hash-plugin (dev only) ---
// This function is appended to SharedSocket.js at serve-time.
// It has access to SharedSocket.js module-scoped variables:
//   sharedWorkerUrl
export function _hotReloadWorker(newHash) {
	if (!('SharedWorker' in window)) return
	console.debug('[SharedSocket] hot-reloading shared worker, hash:', newHash)

	// Trigger full page reload for simplicity - SharedWorker is shared across tabs
	// and we need to ensure all tabs get the new worker
	window.location.reload()
}
