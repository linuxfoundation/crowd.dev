import {fileURLToPath} from 'url'
import {dirname, resolve} from 'path'
import {execute} from '@oclif/core'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const root = resolve(__dirname, '..')

await execute({development: true, dir: root})
