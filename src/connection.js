const sharedWorkerUrl = new URL('./shared-worker.js', import.meta.url)

/** @type {MessagePort | { postMessage, onmessage } | null} */
let port = null
/** topic → Set<callback> */
const listeners = new Map()
/** Guard against concurrent initConnection() calls */
let initPromise = null

/**
 * Wire up the onmessage handler on whatever port-like object we have.
 */
function attachPortListener(p) {
	p.onmessage = (e) => {
		const { type, topic, payload } = e.data

		if (type === 'ping') {
			p.postMessage({ type: 'pong' })
			return
		}

		if (type === 'message' && topic) {
			const cbs = listeners.get(topic)
			if (cbs) {
				for (const cb of cbs) cb(payload, topic)
			}
		}
	}
}

/**
 * Initialise the connection (call once per page).
 * Tries SharedWorker first, falls back to BroadcastChannel + leader election.
 */
export function initConnection() {
	if (typeof window === 'undefined' || port) return port
	if (initPromise) return initPromise
	initPromise = _doInit()
	return initPromise
}

async function _doInit() {
	// 1. Try SharedWorker
	if ('SharedWorker' in window) {
		try {
			const worker = new SharedWorker(sharedWorkerUrl, { type: 'module' })
			port = worker.port
			attachPortListener(port)
			port.start()
			console.debug('[connection] using SharedWorker transport')
			return port
		} catch (error) {
			console.warn('d2l-channel: SharedWorker failed, falling back', error)
		}
	}

	// 2. Fallback: BroadcastChannel + leader election
	try {
		const { createBroadcastTransport } = await import('./service-worker.js')
		port = createBroadcastTransport()
		attachPortListener(port)
		port.start()
		console.debug('[connection] using BroadcastChannel transport (leader election)')
		return port
	} catch (error) {
		console.warn('d2l-channel: BroadcastChannel transport failed', error)
	}

	return null
}

/**
 * Subscribe to a topic. Callback receives (payload, topic).
 * Returns an unsubscribe function.
 */
export function subscribe(topic, callback) {
	if (!listeners.has(topic)) {
		listeners.set(topic, new Set())
	}
	listeners.get(topic).add(callback)

	port?.postMessage({ type: 'subscribe', topic })

	return () => {
		unsubscribe(topic, callback)
	}
}

/**
 * Unsubscribe a specific callback from a topic.
 */
export function unsubscribe(topic, callback) {
	const cbs = listeners.get(topic)
	if (!cbs) return
	cbs.delete(callback)
	if (cbs.size === 0) {
		listeners.delete(topic)
		port?.postMessage({ type: 'unsubscribe', topic })
	}
}

/**
 * Publish a message to a topic (broadcast to other subscribers).
 */
export function publish(topic, payload) {
	port?.postMessage({ type: 'publish', topic, payload })
}
