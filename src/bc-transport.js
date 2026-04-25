/**
 * Fallback transport using broadcast-channel + leader election.
 * Used when SharedWorker is unavailable.
 *
 * All tabs share a BroadcastChannel. One tab is elected leader and opens
 * the single WebSocket to the backend. Non-leader tabs send/receive via
 * the BroadcastChannel; the leader relays between the BC and the WS.
 *
 * Returns a port-like object ({ postMessage, onmessage }) so SharedSocket.js
 * can use it as a drop-in replacement for a SharedWorker MessagePort.
 *
 * Features:
 * - Automatic reconnection with exponential backoff
 * - WebSocket health checks via ping/pong
 * - Resubscription recovery on reconnect
 */
import { BroadcastChannel, createLeaderElection } from 'broadcast-channel'

const BC_NAME = 'lit-channel-transport'
const LEADER_PING_INTERVAL = 30000  // Send pings every 30 seconds
const LEADER_PING_TIMEOUT = 10000   // Expect pong within 10 seconds
const LEADER_PING_MAX_MISSES = 3    // Close after 3 consecutive missed pongs

export function createBroadcastTransport(options = {}) {
	const websocketUrl = options.endpoint ?? `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/api/ws`
	const authToken = typeof options.authToken === 'string' ? options.authToken.trim() : ''
	const bc = new BroadcastChannel(BC_NAME)
	const elector = createLeaderElection(bc)

	let ws = null
	let reconnectTimer = null
	let isLeader = false
	let lastPongTime = 0
	let lastInboundTime = 0
	let pingTimer = null
	let wsHealthCheckInterval = null
	let wsHealthCheckMissCount = 0
    const pendingWsMessages = []

	/** ALL locally-subscribed topics (tracked regardless of leader status) */
	const localTopics = new Set()
	/** topics the server-side WS is subscribed to (managed by leader only) */
	const wsSubscribedTopics = new Set()

	/** The consumer sets this — same shape as MessagePort.onmessage */
	let _onmessage = null

	// ---- WebSocket (leader only) ----

	function startWsHealthCheck() {
		if (wsHealthCheckInterval) return
		console.log('[bc-transport] starting WebSocket health checks')
		lastPongTime = Date.now()
		lastInboundTime = Date.now()
		wsHealthCheckMissCount = 0

		wsHealthCheckInterval = setInterval(() => {
			if (!ws || ws.readyState !== WebSocket.OPEN) {
				stopWsHealthCheck()
				return
			}

			const timeSinceLastPong = Date.now() - lastPongTime
			if (timeSinceLastPong > LEADER_PING_TIMEOUT) {
				wsHealthCheckMissCount++
				console.warn('[bc-transport] ws pong timeout, miss count=%d', wsHealthCheckMissCount)

				if (wsHealthCheckMissCount >= LEADER_PING_MAX_MISSES) {
					console.warn('[bc-transport] ws considered stale after %d missed pongs, reconnecting', wsHealthCheckMissCount)
					stopWsHealthCheck()
					ws.close()
					return
				}
			}

			// Send ping
			try {
				ws.send(JSON.stringify({ type: 'ping' }))
				lastPongTime = Date.now() // will be updated by pong response
			} catch (err) {
				console.warn('[bc-transport] failed to send ping:', err)
			}
		}, LEADER_PING_INTERVAL)
	}

	function stopWsHealthCheck() {
		if (wsHealthCheckInterval) {
			clearInterval(wsHealthCheckInterval)
			wsHealthCheckInterval = null
		}
		if (pingTimer) {
			clearTimeout(pingTimer)
			pingTimer = null
		}
	}

	function connectWebSocket() {
		console.log('[bc-transport] leader connecting WebSocket')
		if (authToken) {
			ws = new WebSocket(websocketUrl, ['bearer', authToken])
		} else {
			ws = new WebSocket(websocketUrl)
		}
		lastPongTime = Date.now()
		lastInboundTime = Date.now()
		wsHealthCheckMissCount = 0

		ws.addEventListener('open', () => {
			console.log('[bc-transport] ws open, resubscribing %d topics', wsSubscribedTopics.size)
			lastPongTime = Date.now()
			lastInboundTime = Date.now()
			startWsHealthCheck()

			for (const topic of wsSubscribedTopics) {
				ws.send(JSON.stringify({ type: 'subscribe', topic }))
			}

			if (pendingWsMessages.length > 0) {
				console.log('[bc-transport] flushing %d queued ws messages', pendingWsMessages.length)
				while (pendingWsMessages.length > 0) {
					const queued = pendingWsMessages.shift()
					ws.send(JSON.stringify(queued))
				}
			}
		})

		ws.addEventListener('message', (e) => {
			lastPongTime = Date.now()  // Update health on any message
			lastInboundTime = Date.now()

			try {
				const msg = JSON.parse(e.data)

				if (msg.type === 'ping') {
					wsSend({ type: 'pong' })
					return
				}

				if (msg.type === 'pong') {
					console.debug('[bc-transport] received pong from server')
					return
				}

				if (msg.type === 'message' && msg.topic) {
					// Relay server messages to all tabs (including ourselves)
					_onmessage?.({ data: msg })
					bc.postMessage(msg)
				}
			} catch (err) {
				console.error('[bc-transport] error parsing message:', err)
			}
		})

		ws.addEventListener('close', () => {
			console.log('[bc-transport] ws closed')
			stopWsHealthCheck()
			scheduleReconnect()
		})

		ws.addEventListener('error', (err) => {
			console.warn('[bc-transport] ws error:', err)
			stopWsHealthCheck()
			ws.close()
		})
	}

	function scheduleReconnect() {
		clearTimeout(reconnectTimer)
		// Exponential backoff: 1s, 2s, 4s up to 30s
		const backoffMs = Math.min(1000 * Math.pow(2, Math.floor(wsHealthCheckMissCount / 2)), 30000)
		console.log('[bc-transport] scheduling reconnect in %dms', backoffMs)
		reconnectTimer = setTimeout(connectWebSocket, backoffMs)
	}

	function wsSend(data) {
		if (ws?.readyState === WebSocket.OPEN) {
			try {
				ws.send(JSON.stringify(data))
				return
			} catch (err) {
				console.error('[bc-transport] failed to send to WebSocket:', err)
			}
		}

		pendingWsMessages.push(data)
		if (pendingWsMessages.length > 512) pendingWsMessages.shift()
	}

	function teardownWebSocket() {
		clearTimeout(reconnectTimer)
		stopWsHealthCheck()
		ws?.close()
		ws = null
	}

	// ---- Leader lifecycle ----

	function becomeLeader() {
		console.log('[bc-transport] became leader')
		isLeader = true
		for (const topic of localTopics) wsSubscribedTopics.add(topic)
		connectWebSocket()
	}

	elector.awaitLeadership().then(becomeLeader)

	// ---- BroadcastChannel listener (messages from other tabs) ----

	bc.onmessage = (msg) => {
		switch (msg.type) {
			case 'publish':
				// Another tab published — deliver locally to our listeners
				_onmessage?.({ data: { type: 'message', topic: msg.topic, payload: msg.payload } })
				// Leader relays to server (server will NOT echo back to us,
				// so no duplicate delivery)
				if (isLeader) wsSend(msg)
				break

			case 'message':
				// Server-originated message relayed by the leader
				_onmessage?.({ data: msg })
				break

			case 'subscribe':
				if (isLeader && !wsSubscribedTopics.has(msg.topic)) {
					wsSubscribedTopics.add(msg.topic)
					wsSend({ type: 'subscribe', topic: msg.topic })
				}
				break

			case 'unsubscribe':
				if (isLeader && wsSubscribedTopics.has(msg.topic)) {
					wsSubscribedTopics.delete(msg.topic)
					wsSend({ type: 'unsubscribe', topic: msg.topic })
				}
				break
		}
	}

	// ---- port-like interface for SharedSocket.js ----

	return {
		/**
		 * Mirror of MessagePort.postMessage
		 * @param {{ type: any; topic: any; payload: any; }} msg
		 */
		postMessage(msg) {
			const { type, topic, payload } = msg

			switch (type) {
				case 'subscribe':
					localTopics.add(topic)
					if (isLeader) {
						wsSubscribedTopics.add(topic)
						wsSend({ type: 'subscribe', topic })
					} else {
						bc.postMessage({ type: 'subscribe', topic })
					}
					break

				case 'unsubscribe':
					localTopics.delete(topic)
					if (isLeader) {
						wsSubscribedTopics.delete(topic)
						wsSend({ type: 'unsubscribe', topic })
					} else {
						bc.postMessage({ type: 'unsubscribe', topic })
					}
					break

				case 'publish':
					// broadcast to other local tabs
					bc.postMessage({ type: 'publish', topic, payload })
					// leader also sends to server
					if (isLeader) {
						wsSend({ type: 'publish', topic, payload })
					}
					break
			}
		},

		/** Mirror of MessagePort.onmessage (setter) */
		set onmessage(fn) { _onmessage = fn },
		get onmessage() { return _onmessage },

		start() { /* no-op, compat with MessagePort */ },

		async close() {
			teardownWebSocket()
			await elector.die()
			await bc.close()
		},
	}
}
