import assert from 'node:assert/strict'
import { test } from 'node:test'

import { main } from './cli.ts'

test('unknown command exits 2', async () => {
  assert.equal(await main(['nope'], {}), 2)
})

test('missing command exits 2', async () => {
  assert.equal(await main([], {}), 2)
})

test('registered handler is dispatched and its exit code returned', async () => {
  const { register } = await import('./cli.ts')
  register('fake', async (env) => (env.X === '1' ? 7 : 0))
  assert.equal(await main(['fake'], { X: '1' }), 7)
})
