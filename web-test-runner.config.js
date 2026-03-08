import { playwrightLauncher } from '@web/test-runner-playwright'
import { wsPlugin } from './dev/ws-plugin.js'
import { matchRoute } from './dev/routes.js'

export default {
	files: 'test/**/*.test.js',
	nodeResolve: {
		browser: true,
		exportConditions: ['browser', 'import'],
	},
	browsers: [
		playwrightLauncher({ product: 'chromium' }),
	],
	plugins: [wsPlugin()],
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
	testRunnerHtml: (testRunnerImport) => `
		<!DOCTYPE html>
		<html>
			<head>
				<meta charset="utf-8" />
				<meta name="viewport" content="width=device-width, initial-scale=1.0" />
			</head>
			<body>
				<script type="module" src="${testRunnerImport}"></script>
			</body>
		</html>
	`,
}
