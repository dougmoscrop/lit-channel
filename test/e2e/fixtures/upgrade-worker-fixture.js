export function installUpgradeWorkerFixture(workerVersion = 'fixture') {
	const ports = new Set()
	const portTopics = new Map()
	const topicResume = new Map()
	let streamSeq = workerVersion === '1' ? 100 : 200

	function normalizeStreamSeq(value) {
		const streamSeqValue = typeof value === 'number'
			? value
			: typeof value === 'string' && value.trim() !== ''
				? Number(value)
				: NaN

		if (!Number.isSafeInteger(streamSeqValue) || streamSeqValue < 0) return undefined
		return streamSeqValue
	}

	function updateResume(topic, value) {
		if (!topic || !value || typeof value !== 'object') return false
		const nextStreamSeq = normalizeStreamSeq(value.streamSeq)
		if (nextStreamSeq === undefined) return false

		const current = topicResume.get(topic)
		if (current && nextStreamSeq <= current.streamSeq) return false

		topicResume.set(topic, {
			streamSeq: nextStreamSeq,
			cursor: value.cursor === undefined || value.cursor === null ? String(nextStreamSeq) : String(value.cursor),
			sessionId: value.sessionId === undefined || value.sessionId === null ? undefined : String(value.sessionId),
		})
		return true
	}

	function envelope(payload) {
		streamSeq += 1
		return {
			...payload,
			workerVersion,
			__rt: {
				streamSeq,
				eventId: `fixture-${workerVersion}-${streamSeq}-${payload.step || 'message'}`,
			},
		}
	}

	function topicsForPort(port) {
		const topics = []
		for (const topic of portTopics.get(port) || []) {
			const resume = topicResume.get(topic)
			topics.push(resume ? { topic, resume: { ...resume } } : { topic })
		}
		return topics
	}

	function deliver(topic, payload) {
		for (const port of ports) {
			if (portTopics.get(port)?.has(topic)) {
				port.postMessage({ type: 'message', topic, payload: envelope(payload) })
			}
		}
	}

	function removePort(port) {
		ports.delete(port)
		portTopics.delete(port)
	}

	self.addEventListener('connect', (event) => {
		const port = event.ports[0]
		ports.add(port)
		portTopics.set(port, new Set())

		port.addEventListener('message', (messageEvent) => {
			const message = messageEvent.data
			const { type, topic } = message

			switch (type) {
				case 'config':
					for (const entry of message.upgrade?.topics || []) {
						if (!entry?.topic) continue
						portTopics.get(port).add(entry.topic)
						updateResume(entry.topic, entry.resume)
					}
					if (message.upgrade?.upgradeId) {
						port.postMessage({
							type: 'lit-channel:worker-ready',
							upgradeId: message.upgrade.upgradeId,
							workerVersion,
						})
					}
					break

				case 'subscribe':
					portTopics.get(port).add(topic)
					updateResume(topic, message.resume)
					break

				case 'ack':
					updateResume(topic, message)
					break

				case 'publish':
					deliver(topic, message.payload)
					break

				case 'pong':
					break

				case 'lit-channel:prepare-worker-upgrade':
					for (const entry of topicsForPort(port)) {
						deliver(entry.topic, { step: 'old-inflight' })
					}
					port.postMessage({
						type: 'lit-channel:worker-upgrade-state',
						upgradeId: message.upgradeId,
						workerVersion,
						topics: topicsForPort(port),
						pending: [],
					})
					break

				case 'lit-channel:complete-worker-upgrade':
					removePort(port)
					port.close()
					break
			}
		})

		port.start()
	})
}
