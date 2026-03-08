/**
 * Test API routes for local development.
 * Add handlers here — they are served under /api by the dev server middleware.
 */

const routes = new Map()

// ---------- helpers ----------

function json(ctx, body, status = 200) {
	ctx.status = status
	ctx.set('Content-Type', 'application/json')
	ctx.body = JSON.stringify(body)
}

async function parseBody(ctx) {
	return new Promise((resolve, reject) => {
		let data = ''
		ctx.req.on('data', chunk => (data += chunk))
		ctx.req.on('end', () => {
			try {
				resolve(data ? JSON.parse(data) : {})
			} catch {
				reject(new Error('Invalid JSON'))
			}
		})
		ctx.req.on('error', reject)
	})
}

// ---------- in-memory store ----------

let channels = [
	{ id: '1', name: 'Announcements', description: 'Latest updates from your instructor' },
	{ id: '2', name: 'General', description: 'Open discussion' },
]

// ---------- route definitions ----------

routes.set('GET /api/channels', (ctx) => {
	json(ctx, channels)
})

routes.set('GET /api/channels/:id', (ctx, params) => {
	const channel = channels.find(c => c.id === params.id)
	if (!channel) return json(ctx, { error: 'Not found' }, 404)
	json(ctx, channel)
})

routes.set('POST /api/channels', async (ctx) => {
	const body = await parseBody(ctx)
	const channel = {
		id: String(Date.now()),
		name: body.name ?? 'Untitled',
		description: body.description ?? '',
	}
	channels.push(channel)
	json(ctx, channel, 201)
})

routes.set('DELETE /api/channels/:id', (ctx, params) => {
	const before = channels.length
	channels = channels.filter(c => c.id !== params.id)
	if (channels.length === before) return json(ctx, { error: 'Not found' }, 404)
	json(ctx, { ok: true })
})

// ---------- matcher ----------

/**
 * Resolve the matching route for a given method + path.
 * Supports simple `:param` segments.
 */
export function matchRoute(method, path) {
	// try exact match first
	const exact = routes.get(`${method} ${path}`)
	if (exact) return { handler: exact, params: {} }

	// try parameterised routes
	for (const [pattern, handler] of routes) {
		const [routeMethod, routePath] = pattern.split(' ')
		if (routeMethod !== method) continue

		const routeParts = routePath.split('/')
		const pathParts = path.split('/')
		if (routeParts.length !== pathParts.length) continue

		const params = {}
		const match = routeParts.every((seg, i) => {
			if (seg.startsWith(':')) {
				params[seg.slice(1)] = pathParts[i]
				return true
			}
			return seg === pathParts[i]
		})

		if (match) return { handler, params }
	}

	return null
}
