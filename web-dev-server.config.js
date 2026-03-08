import { sharedWorkerPlugin } from './dev/shared-worker-plugin.js'
import { matchRoute } from './dev/routes.js'
import { wsPlugin } from './dev/ws-plugin.js'

export default {
	nodeResolve: {
		browser: true,
		exportConditions: ['browser', 'import'],
	},
	open: false,
	watch: true,
	appIndex: 'index.html',
	plugins: [sharedWorkerPlugin(), wsPlugin()],
	middleware: [
		// Serve /api/* requests from the test API routes
		async function apiMiddleware(ctx, next) {
			if (!ctx.url.startsWith('/api')) {
				return next()
			}

			const result = matchRoute(ctx.method, ctx.url.split('?')[0])
			if (!result) {
				ctx.status = 404
				ctx.set('Content-Type', 'application/json')
				ctx.body = JSON.stringify({ error: 'API route not found' })
				return
			}

			await result.handler(ctx, result.params)
		},
	],
}
