import assert from 'node:assert/strict'
import { test } from 'node:test'

import { main, register } from './cli.ts'

test('unknown command exits 2', async () => {
  assert.equal(await main(['nope'], {}), 2)
})

test('missing command exits 2', async () => {
  assert.equal(await main([], {}), 2)
})

test('registered handler is dispatched and its exit code returned', async () => {
  register('cli-test-dispatch', async (env) => (env.X === '1' ? 7 : 0))
  assert.equal(await main(['cli-test-dispatch'], { X: '1' }), 7)
})

test('a throwing handler is reported on stderr and exits 1', async (t) => {
  const errors: string[] = []
  t.mock.method(console, 'error', (msg: string) => errors.push(msg))
  register('cli-test-throws', async () => {
    throw new Error('boom')
  })
  assert.equal(await main(['cli-test-throws'], {}), 1)
  assert.deepEqual(errors, ['error: boom'])
})
