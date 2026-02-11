import { WebSocketServer } from 'ws'

/**
 * Web Dev Server plugin that adds a WebSocket endpoint at /api/ws
 * with topic-based pub-sub.
 *
 * Protocol (JSON messages):
 *
 *   → { type: "subscribe",   topic: "announcements" }
 *   → { type: "unsubscribe", topic: "announcements" }
 *   → { type: "publish",     topic: "announcements", payload: { … } }
 *
 *   ← { type: "message",     topic: "announcements", payload: { … } }
 *
 * A publish is delivered to all subscribers of that topic *except* the sender.
 */
export function wsPlugin() {
	/** @type {WebSocketServer} */
	let wss

	/** topic → Set<WebSocket> */
	const subscriptions = new Map()

	function subscribeTo(topic, ws) {
		if (!subscriptions.has(topic)) {
			subscriptions.set(topic, new Set())
		}
		subscriptions.get(topic).add(ws)
	}

	function unsubscribeFrom(topic, ws) {
		const subs = subscriptions.get(topic)
		if (!subs) return
		subs.delete(ws)
		if (subs.size === 0) subscriptions.delete(topic)
	}

	function unsubscribeAll(ws) {
		for (const [topic, subs] of subscriptions) {
			subs.delete(ws)
			if (subs.size === 0) subscriptions.delete(topic)
		}
	}

	function publish(topic, payload, sender) {
		const subs = subscriptions.get(topic)
		if (!subs) return
		const msg = JSON.stringify({ type: 'message', topic, payload })
		for (const client of subs) {
			if (client !== sender && client.readyState === 1 /* OPEN */) {
				client.send(msg)
			}
		}
	}

	return {
		name: 'ws-api',

		serverStart({ app, server }) {
			wss = new WebSocketServer({ noServer: true })

			server.on('upgrade', (req, socket, head) => {
				if (req.url === '/api/ws') {
					wss.handleUpgrade(req, socket, head, (ws) => {
						wss.emit('connection', ws, req)
					})
				}
				// let other upgrade requests (e.g. WDS hot-reload) pass through
			})

			wss.on('connection', (ws) => {

				ws.on('message', (raw) => {
					let msg
					try {
						msg = JSON.parse(raw)
					} catch {
						ws.send(JSON.stringify({ type: 'error', error: 'invalid JSON' }))
						return
					}

					const { type, topic, payload } = msg

					if (!topic) {
						ws.send(JSON.stringify({ type: 'error', error: 'missing topic' }))
						return
					}

					switch (type) {
						case 'subscribe':
							subscribeTo(topic, ws)
							break

						case 'unsubscribe':
							unsubscribeFrom(topic, ws)
							break

						case 'publish':
							publish(topic, payload, ws)
							break

						default:
							ws.send(JSON.stringify({ type: 'error', error: `unknown type: ${type}` }))
					}
				})

				ws.on('close', () => {
					unsubscribeAll(ws)
				})
			})
		},

		serverStop() {
			wss?.close()
		},
	}
}
