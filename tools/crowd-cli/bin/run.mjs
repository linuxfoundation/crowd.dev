#!/usr/bin/env node

// Suppress known false-positive from @oclif/plugin-autocomplete (UnparsedCommand bug).
// Must run before oclif imports so it intercepts before oclif's own warning listener registers.
const _origEmitWarning = process.emitWarning.bind(process)
process.emitWarning = (warning, ...args) => {
  const code = typeof args[0] === 'object' ? args[0]?.code : undefined
  if (code === 'UnparsedCommand') return
  _origEmitWarning(warning, ...args)
}

import {fileURLToPath} from 'node:url'
import {dirname, resolve} from 'node:path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const root = resolve(__dirname, '..')

// Route bare `crowd` invocation to the dashboard when running in a terminal.
// oclif's `default` config requires a manifest file; this is more reliable.
if (process.argv.length === 2 && process.stdout.isTTY) {
  process.argv.push('dashboard')
}

const {execute} = await import('@oclif/core')
await execute({development: true, dir: root})
