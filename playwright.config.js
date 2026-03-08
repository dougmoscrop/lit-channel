import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
	testDir: './test/e2e',
	testMatch: '*.spec.js',
	fullyParallel: true,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 2 : 0,
	workers: process.env.CI ? 1 : undefined,

	outputDir: './test/e2e/results',
	reporter: [['html', { open: 'never', outputFolder: './test/e2e/report' }]],

	use: {
		headless: true,
		baseURL: 'http://localhost:8000',
		trace: 'on-first-retry',
		screenshot: 'only-on-failure',
	},

	projects: [
		{
			name: 'chromium',
			use: { ...devices['Desktop Chrome'] },
		},
	],

	webServer: {
		command: 'npx web-dev-server',
		url: 'http://localhost:8000',
		reuseExistingServer: !process.env.CI,
		timeout: 120 * 1000,
	},
})
