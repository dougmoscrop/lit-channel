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
 */
import { BroadcastChannel, createLeaderElection } from 'broadcast-channel'

const BC_NAME = 'lit-channel-transport'

export function createBroadcastTransport(options = {}) {
	const websocketUrl = options.endpoint ?? `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/api/ws`
	const bc = new BroadcastChannel(BC_NAME)
	const elector = createLeaderElection(bc)

	let ws = null
	let reconnectTimer = null
	let isLeader = false

	/** ALL locally-subscribed topics (tracked regardless of leader status) */
	const localTopics = new Set()
	/** topics the server-side WS is subscribed to (managed by leader only) */
	const wsSubscribedTopics = new Set()

	/** The consumer sets this — same shape as MessagePort.onmessage */
	let _onmessage = null

	// ---- WebSocket (leader only) ----

	function connectWebSocket() {
		ws = new WebSocket(websocketUrl)

		ws.addEventListener('open', () => {
			for (const topic of wsSubscribedTopics) {
				ws.send(JSON.stringify({ type: 'subscribe', topic }))
			}
		})

		ws.addEventListener('message', (e) => {
			const msg = JSON.parse(e.data)
			if (msg.type === 'message' && msg.topic) {
				// Relay server messages to all tabs (including ourselves)
				_onmessage?.({ data: msg })
				bc.postMessage(msg)
			}
		})

		ws.addEventListener('close', () => scheduleReconnect())
		ws.addEventListener('error', () => ws.close())
	}

	function scheduleReconnect() {
		clearTimeout(reconnectTimer)
		reconnectTimer = setTimeout(connectWebSocket, 3000)
	}

	function wsSend(data) {
		if (ws?.readyState === WebSocket.OPEN) {
			ws.send(JSON.stringify(data))
		}
	}

	function teardownWebSocket() {
		clearTimeout(reconnectTimer)
		ws?.close()
		ws = null
	}

	// ---- Leader lifecycle ----

	function becomeLeader() {
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
