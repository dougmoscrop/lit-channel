console.log('[shared-worker] loaded')

/** @type {Map<MessagePort, boolean>} port → alive flag (set true on pong) */
const ports = new Map()
/** topic → Set<MessagePort> */
const topicPorts = new Map()

let ws
let reconnectTimer
let heartbeatInterval
let endpoint = `${self.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${self.location.host}/api/ws`

const HEARTBEAT_MS = 30000

// ---------- WebSocket ----------

function connectWebSocket() {
	console.log('[shared-worker] connecting ws...')
	ws = new WebSocket(endpoint)

	ws.addEventListener('open', () => {
		console.log('[shared-worker] ws open, resubscribing %d topics', topicPorts.size)
		for (const topic of topicPorts.keys()) {
			ws.send(JSON.stringify({ type: 'subscribe', topic }))
		}
	})

	ws.addEventListener('message', (e) => {
		const msg = JSON.parse(e.data)
		console.log('[shared-worker] ws message:', msg)
		if (msg.type === 'message' && msg.topic) {
			const subs = topicPorts.get(msg.topic)
			console.log('[shared-worker] delivering topic=%s to %d ports', msg.topic, subs?.size ?? 0)
			if (subs) {
				for (const port of subs) port.postMessage(msg)
			}
		}
	})

	ws.addEventListener('close', () => {
		console.log('[shared-worker] ws closed')
		scheduleReconnect()
	})
	ws.addEventListener('error', () => ws.close())
}

function scheduleReconnect() {
	clearTimeout(reconnectTimer)
	reconnectTimer = setTimeout(connectWebSocket, 3000)
}

function send(data) {
	console.log('[shared-worker] send:', data, 'ws.readyState=%s', ws?.readyState)
	if (ws?.readyState === WebSocket.OPEN) {
		ws.send(JSON.stringify(data))
	}
}

connectWebSocket()

// ---------- Port (tab) management ----------

function handlePortMessage(port, msg) {
	console.log('[shared-worker] port message:', msg)
	const { type, topic, endpoint: nextEndpoint } = msg

	switch (type) {
		case 'config': {
			if (!nextEndpoint || nextEndpoint === endpoint) {
				break
			}

			endpoint = nextEndpoint
			clearTimeout(reconnectTimer)
			if (ws?.readyState === WebSocket.OPEN || ws?.readyState === WebSocket.CONNECTING) {
				ws.close()
			} else {
				connectWebSocket()
			}
			break
		}
		case 'pong': {
			// mark port as alive
			ports.set(port, true)
			break
		}
		case 'subscribe': {
			if (!topicPorts.has(topic)) {
				topicPorts.set(topic, new Set())
				// first local subscriber → tell the server
				send({ type: 'subscribe', topic })
			}
			topicPorts.get(topic).add(port)
			break
		}
		case 'unsubscribe': {
			const subs = topicPorts.get(topic)
			if (subs) {
				subs.delete(port)
				if (subs.size === 0) {
					topicPorts.delete(topic)
					// last local subscriber → tell the server
					send({ type: 'unsubscribe', topic })
				}
			}
			break
		}
		case 'publish': {
			send(msg)
			break
		}
	}
}

function removePort(port) {
	console.log('[shared-worker] removing port (%d before)', ports.size)
	ports.delete(port)
	for (const [topic, subs] of topicPorts) {
		subs.delete(port)
		if (subs.size === 0) {
			topicPorts.delete(topic)
			send({ type: 'unsubscribe', topic })
		}
	}
	if (ports.size === 0) {
		console.log('[shared-worker] no ports left, stopping heartbeat')
		stopHeartbeat()
	}
}

// ---------- Heartbeat ----------

function startHeartbeat() {
	if (heartbeatInterval) return
	console.log('[shared-worker] starting heartbeat (every %ds)', HEARTBEAT_MS / 1000)
	heartbeatInterval = setInterval(() => {
		console.log('[shared-worker] heartbeat: %d ports', ports.size)
		for (const [port, alive] of ports) {
			if (!alive) {
				removePort(port)
			} else {
				// reset flag and send next ping
				ports.set(port, false)
				try {
					port.postMessage({ type: 'ping' })
				} catch {
					// postMessage can throw if port is neutered
					removePort(port)
				}
			}
		}
	}, HEARTBEAT_MS)
}

function stopHeartbeat() {
	clearInterval(heartbeatInterval)
	heartbeatInterval = null
}

/**
 * @param {MessageEvent} e
 */
function onConnect(e) {
	const port = e.ports[0]
	if (!ws || ws.readyState === WebSocket.CLOSED) {
		connectWebSocket()
	}
	ports.set(port, true)
	console.log('[shared-worker] tab connected (%d total)', ports.size)

	port.addEventListener('message', (m) => handlePortMessage(port, m.data))
	port.start()
	startHeartbeat()
}

self.addEventListener('connect', onConnect)
