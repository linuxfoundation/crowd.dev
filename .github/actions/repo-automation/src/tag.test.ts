import assert from 'node:assert/strict'
import { test } from 'node:test'

import { planTags } from './tag.ts'

const c = (sha: string, title: string, extra = {}) => ({
  sha,
  title,
  number: 1,
  author: 'a',
  paths: ['docs/x.md'],
  existingTag: null,
  ...extra,
})

test('tags every untagged commit since last tag', () => {
  const p = planTags(
    [c('a', 'fix: x'), c('b', 'feat: y'), c('c', 'chore: z')],
    'v0.50.0',
    'v0.50.0',
    [],
  )
  assert.deepEqual(
    p.map((t) => t.version),
    ['v0.50.1', 'v0.51.0', 'v0.51.1'],
  )
  assert.deepEqual(
    p.map((t) => t.release),
    [true, true, false],
  )
})

test('skips commits that already have a tag', () => {
  const p = planTags(
    [c('a', 'fix: x', { existingTag: 'v0.50.1' }), c('b', 'fix: y')],
    'v0.50.0',
    'v0.50.0',
    [],
  )
  assert.deepEqual(
    p.map((t) => [t.sha, t.version]),
    [['b', 'v0.50.2']],
  )
})

test('first ever run uses initial version', () =>
  assert.equal(planTags([c('a', 'feat: x')], null, 'v0.50.0', [])[0].version, 'v0.50.0'))

test('first run chains from initial for later commits', () => {
  const p = planTags([c('a', 'feat: x'), c('b', 'fix: y')], null, 'v0.1.0', [])
  assert.deepEqual(
    p.map((t) => t.version),
    ['v0.1.0', 'v0.1.1'],
  )
})

test('body carries title, number, author and services from rules', () => {
  const p = planTags(
    [c('a', 'feat: x', { number: 9, author: 'z', paths: ['backend/src/a.ts'] })],
    'v0.50.0',
    'v0.50.0',
    [{ prefix: 'backend/', services: ['api'] }],
  )
  assert.equal(p[0].title, 'feat: x')
  assert.equal(p[0].body, 'feat: x (#9) — @z\nServices: api\n<!-- release-services: ["api"] -->')
})
