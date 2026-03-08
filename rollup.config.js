import { nodeResolve } from '@rollup/plugin-node-resolve'
import { importMetaAssets } from '@web/rollup-plugin-import-meta-assets'

export default {
  input: 'src/lit-channel.js',
  output: {
    format: 'es',
    sourcemap: false,
    entryFileNames: '[name]-[hash].js',
    chunkFileNames: '[name]-[hash].js'
  },
  plugins: [
    nodeResolve({ browser: true }),
    importMetaAssets()
  ]
}
