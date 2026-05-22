export default [
	{
		files: ['**/*.js', '**/*.mjs'],
		ignores: ['dist/**', 'node_modules/**'],
		rules: {
			semi: ['error', 'never'],
		},
	},
]
