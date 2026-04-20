import { nodeResolve } from '@rollup/plugin-node-resolve'
import { importMetaAssets } from '@web/rollup-plugin-import-meta-assets'

const isLitExternal = (id) =>
  /^lit($|\/)|^lit-.+|^@lit(\/|-)/.test(id)

export default {
  input: 'src/lit-channel.js',
  external: ['lit'],
  output: {
    dir: 'dist',
    format: 'es',
    sourcemap: false,
    entryFileNames: '[name].js',
    chunkFileNames: '[name]-[hash].js',
  },
  plugins: [
    nodeResolve({ browser: true }),
    importMetaAssets()
  ]
}
