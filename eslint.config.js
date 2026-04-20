export default [
	{
		files: ['**/*.js', '**/*.mjs'],
		ignore: ['dist/**', 'node_modules/**'],
		rules: {
			semi: ['error', 'never'],
		},
	},
]
