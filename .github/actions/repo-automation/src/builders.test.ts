import assert from 'node:assert/strict'
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, before, test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { loadBuilderServiceMap, toBuilders } from './builders.ts'

let dir = ''
before(() => {
  dir = mkdtempSync(join(tmpdir(), 'builders-'))
  writeFileSync(
    join(dir, 'backend.env'),
    'REPO="x/backend"\nSERVICES="api api-e2e job-generator"\n',
  )
  writeFileSync(join(dir, 'frontend.env'), "SERVICES='frontend'\n")
  writeFileSync(join(dir, 'cache-worker.env'), 'SERVICES=cache-worker')
  writeFileSync(join(dir, 'redis.env'), 'REPO="x/redis"\n')
  writeFileSync(join(dir, 'notes.txt'), 'SERVICES="ignored"\n')
})
after(() => rmSync(dir, { recursive: true, force: true }))

test('maps each service to its builder, quotes optional', () => {
  const map = loadBuilderServiceMap(dir)
  assert.equal(map.get('api'), 'backend')
  assert.equal(map.get('job-generator'), 'backend')
  assert.equal(map.get('frontend'), 'frontend')
  assert.equal(map.get('cache-worker'), 'cache-worker')
  assert.equal(map.has('ignored'), false)
})

test('dedupes and sorts builders', () => {
  const map = loadBuilderServiceMap(dir)
  assert.deepEqual(toBuilders(['api', 'job-generator'], map), {
    builders: ['backend'],
    unknown: [],
  })
  assert.deepEqual(toBuilders(['frontend', 'api'], map).builders, ['backend', 'frontend'])
})

test('unknown names pass through and are reported', () => {
  const map = loadBuilderServiceMap(dir)
  assert.deepEqual(toBuilders(['api', 'mystery'], map), {
    builders: ['backend', 'mystery'],
    unknown: ['mystery'],
  })
})

test('real scripts/builders maps api to backend', (t) => {
  const real = fileURLToPath(new URL('../../../../scripts/builders', import.meta.url))
  if (!existsSync(real)) return t.skip('scripts/builders not found')
  assert.equal(loadBuilderServiceMap(real).get('api'), 'backend')
})
