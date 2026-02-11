/** @type {Map<MessagePort, boolean>} port → alive flag (set true on pong) */
const ports = new Map()
/** topic → Set<MessagePort> */
const topicPorts = new Map()

let ws
let reconnectTimer
let heartbeatInterval

const HEARTBEAT_MS = 5000

// ---------- WebSocket ----------

function connectWebSocket() {
	ws = new WebSocket(`ws://${self.location.host}/api/ws`)

	ws.addEventListener('open', () => {
		for (const topic of topicPorts.keys()) {
			ws.send(JSON.stringify({ type: 'subscribe', topic }))
		}
	})

	ws.addEventListener('message', (e) => {
		const msg = JSON.parse(e.data)
		if (msg.type === 'message' && msg.topic) {
			const subs = topicPorts.get(msg.topic)
			if (subs) {
				for (const port of subs) port.postMessage(msg)
			}
		}
	})

	ws.addEventListener('close', () => scheduleReconnect())
	ws.addEventListener('error', () => ws.close())
}

function scheduleReconnect() {
	clearTimeout(reconnectTimer)
	reconnectTimer = setTimeout(connectWebSocket, 3000)
}

function send(data) {
	if (ws?.readyState === WebSocket.OPEN) {
		ws.send(JSON.stringify(data))
	}
}

connectWebSocket()

// ---------- Port (tab) management ----------

function handlePortMessage(port, msg) {
	const { type, topic } = msg

	switch (type) {
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
	ports.delete(port)
	for (const [topic, subs] of topicPorts) {
		subs.delete(port)
		if (subs.size === 0) {
			topicPorts.delete(topic)
			send({ type: 'unsubscribe', topic })
		}
	}
	if (ports.size === 0) stopHeartbeat()
}

// ---------- Heartbeat ----------

function startHeartbeat() {
	if (heartbeatInterval) return
	heartbeatInterval = setInterval(() => {
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

self.onconnect = (e) => {
	const port = e.ports[0]
	ports.set(port, true)

	port.onmessage = (m) => handlePortMessage(port, m.data)
	port.start()
	startHeartbeat()
}
