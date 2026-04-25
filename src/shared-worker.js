/** @type {Map<MessagePort, boolean>} port → alive flag (set true on pong) */
const ports = new Map()
/** topic → Set<MessagePort> */
const topicPorts = new Map()
/** topic → latest resume cursor known to the worker */
const topicResume = new Map()

let ws
let reconnectTimer
let heartbeatInterval
let wsHealthCheckInterval
let lastWsPingAt = 0
let lastWsInboundAt = 0
let awaitingWsPong = false
let wsHealthCheckMissCount = 0
const pendingWsMessages = []
let endpoint = `${self.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${self.location.host}/api/ws`
let authToken = ''

const HEARTBEAT_MS = 30000
const WS_PING_INTERVAL_MS = 30000
const WS_PONG_TIMEOUT_MS = 10000
const WS_HEALTH_CHECK_MAX_MISSES = 3

function normalizeStreamSeq(value) {
	const streamSeq = typeof value === 'number'
		? value
		: typeof value === 'string' && value.trim() !== ''
			? Number(value)
			: NaN

	if (!Number.isSafeInteger(streamSeq) || streamSeq < 0) return undefined
	return streamSeq
}

function updateTopicResume(topic, value) {
	if (!topic || !value || typeof value !== 'object') return false
	const streamSeq = normalizeStreamSeq(value.streamSeq)
	if (streamSeq === undefined) return false

	const current = topicResume.get(topic)
	if (current && streamSeq <= current.streamSeq) return false

	const cursor = value.cursor === undefined || value.cursor === null
		? String(streamSeq)
		: String(value.cursor)
	const sessionId = value.sessionId === undefined || value.sessionId === null
		? undefined
		: String(value.sessionId)

	topicResume.set(topic, { streamSeq, cursor, sessionId })
	return true
}

function buildSubscribeFrame(topic) {
	const frame = { type: 'subscribe', topic }
	const resume = topicResume.get(topic)
	if (resume) {
		frame.resume = { ...resume }
	}
	return frame
}

// ---------- WebSocket ----------

function startWsHealthCheck() {
	if (wsHealthCheckInterval) return
	console.log('[shared-worker] starting ws health checks (every %dms)', WS_PING_INTERVAL_MS)
	lastWsPingAt = 0
	lastWsInboundAt = Date.now()
	awaitingWsPong = false
	wsHealthCheckMissCount = 0

	wsHealthCheckInterval = setInterval(() => {
		if (!ws || ws.readyState !== WebSocket.OPEN) {
			stopWsHealthCheck()
			return
		}

		const now = Date.now()

		// Check for pong timeout
		if (awaitingWsPong && now - lastWsPingAt > WS_PONG_TIMEOUT_MS) {
			awaitingWsPong = false
			wsHealthCheckMissCount++
			console.warn('[shared-worker] ws pong timeout, miss count=%d', wsHealthCheckMissCount)
			if (wsHealthCheckMissCount >= WS_HEALTH_CHECK_MAX_MISSES) {
				console.warn('[shared-worker] ws considered stale after %d missed pongs, reconnecting', wsHealthCheckMissCount)
				stopWsHealthCheck()
				ws.close()
				return
			}
		}

		// Send ping when enough time has passed and we're not waiting for a pong
		if (!awaitingWsPong) {
			lastWsPingAt = now
			awaitingWsPong = true
			send({ type: 'ping' })
		}
	}, WS_PING_INTERVAL_MS)
}

function stopWsHealthCheck() {
	if (wsHealthCheckInterval) {
		clearInterval(wsHealthCheckInterval)
		wsHealthCheckInterval = null
	}
}

function connectWebSocket() {
	console.log('[shared-worker] connecting ws...')
	if (authToken) {
		ws = new WebSocket(endpoint, ['bearer', authToken])
	} else {
		ws = new WebSocket(endpoint)
	}
	lastWsPingAt = 0
	lastWsInboundAt = Date.now()
	awaitingWsPong = false
	wsHealthCheckMissCount = 0

	ws.addEventListener('open', () => {
		console.log('[shared-worker] ws open, resubscribing %d topics', topicPorts.size)
		lastWsPingAt = Date.now() - WS_PING_INTERVAL_MS
		lastWsInboundAt = Date.now()
		startWsHealthCheck()
		for (const topic of topicPorts.keys()) {
			ws.send(JSON.stringify(buildSubscribeFrame(topic)))
		}
		// Flush any queued outbound messages that were published while reconnecting.
		if (pendingWsMessages.length > 0) {
			console.log('[shared-worker] flushing %d queued ws messages', pendingWsMessages.length)
			while (pendingWsMessages.length > 0) {
				const queued = pendingWsMessages.shift()
				ws.send(JSON.stringify(queued))
			}
		}
	})

	ws.addEventListener('message', (e) => {
		lastWsInboundAt = Date.now()
		const msg = JSON.parse(e.data)
		console.log('[shared-worker] ws message:', msg)
		if (msg.type === 'ping') {
			send({ type: 'pong' })
			return
		}
		if (msg.type === 'pong') {
			awaitingWsPong = false
			wsHealthCheckMissCount = 0
			return
		}
		if (msg.type === 'message' && msg.topic) {
			const subs = topicPorts.get(msg.topic)
			console.log('[shared-worker] delivering topic=%s to %d ports', msg.topic, subs?.size ?? 0)
			if (subs) {
				for (const port of subs) port.postMessage(msg)
			}
		}
	})

	ws.addEventListener('close', (event) => {
		console.log('[shared-worker] ws closed code=%s reason=%s clean=%s', event?.code, event?.reason || '', event?.wasClean)
		stopWsHealthCheck()
		scheduleReconnect()
	})
	ws.addEventListener('error', () => {
		console.warn('[shared-worker] ws error')
		stopWsHealthCheck()
		ws.close()
	})
}

function scheduleReconnect() {
	clearTimeout(reconnectTimer)
	// Exponential backoff: 1s, 2s, 4s up to 30s
	const backoffMs = Math.min(1000 * Math.pow(2, Math.floor(wsHealthCheckMissCount / 2)), 30000)
	console.log('[shared-worker] scheduling reconnect in %dms', backoffMs)
	reconnectTimer = setTimeout(connectWebSocket, backoffMs)
}

function send(data) {
	console.log('[shared-worker] send:', data, 'ws.readyState=%s', ws?.readyState)
	if (ws?.readyState === WebSocket.OPEN) {
		ws.send(JSON.stringify(data))
		return
	}

	// Queue outbound operations during reconnect windows so transient outages
	// do not drop short-lived signals like typing indicators.
	if (data?.type === 'publish' || data?.type === 'subscribe' || data?.type === 'unsubscribe' || data?.type === 'ack' || data?.type === 'ping' || data?.type === 'pong') {
		pendingWsMessages.push(data)
		// Keep queue bounded to avoid unbounded growth under prolonged disconnects.
		if (pendingWsMessages.length > 512) pendingWsMessages.shift()
	}
}

connectWebSocket()

// ---------- Port (tab) management ----------

function handlePortMessage(port, msg) {
	console.log('[shared-worker] port message:', msg)
	const { type, topic, endpoint: nextEndpoint } = msg

	switch (type) {
		case 'config': {
			const hasAuthToken = Object.prototype.hasOwnProperty.call(msg, 'authToken')
			const nextAuthToken = hasAuthToken && typeof msg.authToken === 'string'
				? msg.authToken.trim()
				: ''
			const hasEndpointChange = Boolean(nextEndpoint && nextEndpoint !== endpoint)
			const hasAuthTokenChange = hasAuthToken && nextAuthToken !== authToken

			if (!hasEndpointChange && !hasAuthTokenChange) {
				break
			}

			if (hasEndpointChange) {
				endpoint = nextEndpoint
			}
			if (hasAuthTokenChange) {
				authToken = nextAuthToken
			}
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
			const didAdvance = updateTopicResume(topic, msg.resume)
			if (!topicPorts.has(topic)) {
				topicPorts.set(topic, new Set())
				// first local subscriber → tell the server
				send(buildSubscribeFrame(topic))
			} else if (didAdvance) {
				send(buildSubscribeFrame(topic))
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
					topicResume.delete(topic)
					// last local subscriber → tell the server
					send({ type: 'unsubscribe', topic })
				}
			}
			break
		}
		case 'ack': {
			updateTopicResume(topic, msg)
			send(msg)
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
			topicResume.delete(topic)
			send({ type: 'unsubscribe', topic })
		}
	}
	if (ports.size === 0) {
		console.log('[shared-worker] no ports left, stopping heartbeat and ws health checks')
		stopHeartbeat()
		stopWsHealthCheck()
	}
}

// ---------- Heartbeat ----------

function startHeartbeat() {
	if (heartbeatInterval) return
	console.log('[shared-worker] starting heartbeat (every %ds)', HEARTBEAT_MS / 1000)
	heartbeatInterval = setInterval(() => {
		console.log('[shared-worker] heartbeat: %d ports', ports.size)
		const deadPorts = []
		for (const [port, alive] of ports) {
			if (!alive) {
				console.log('[shared-worker] port failed pong, removing')
				deadPorts.push(port)
			} else {
				// reset flag and send next ping
				ports.set(port, false)
				try {
					port.postMessage({ type: 'ping' })
				} catch (err) {
					// postMessage can throw if port is neutered
					console.warn('[shared-worker] failed to send ping to port:', err)
					deadPorts.push(port)
				}
			}
		}
		// Remove dead ports after iteration
		for (const port of deadPorts) {
			removePort(port)
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
