import assert from 'node:assert/strict'
import { test } from 'node:test'

import { applyPlan, planTags, resolveBase } from './tag.ts'
import type { PlannedTag } from './tag.ts'

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

test('resolveBase ignores a base older than the initial version', () => {
  assert.equal(resolveBase('v0.49.0', 'v0.50.0'), null)
  assert.equal(resolveBase('v0.50.0', 'v0.50.0'), 'v0.50.0')
  assert.equal(resolveBase('v0.51.2', 'v0.50.0'), 'v0.51.2')
  assert.equal(resolveBase(null, 'v0.50.0'), null)
})

test('a base below initial plans exactly the initial version', () => {
  const base = resolveBase('v0.49.0', 'v0.50.0')
  assert.equal(planTags([c('a', 'fix: x')], base, 'v0.50.0', [])[0].version, 'v0.50.0')
})

const plan = (version: string, release: boolean, sha = version): PlannedTag => ({
  sha,
  version,
  release,
  title: 't',
  body: 'b',
})

const fakeRunner =
  (calls: string[][], failOn?: (args: string[]) => boolean) =>
  (cmd: string, args: string[]): string => {
    calls.push([cmd, ...args])
    if (failOn?.([cmd, ...args])) throw new Error('nope')
    return ''
  }

const kind = (call: string[]): string => call.slice(0, 3).join(' ')

test('drafts are created before the single atomic push', () => {
  const calls: string[][] = []
  const planned = [plan('v0.51.0', true), plan('v0.51.1', false), plan('v0.52.0', true)]
  assert.equal(
    applyPlan(
      planned,
      fakeRunner(calls, (a) => a[2] === 'view'),
    ),
    0,
  )
  const kinds = calls.map(kind)
  const pushAt = kinds.findIndex((k) => k.startsWith('git push'))
  assert.equal(kinds.filter((k) => k.startsWith('git push')).length, 1)
  assert.equal(pushAt, kinds.length - 1)
  assert.equal(kinds.filter((k) => k === 'gh release create').length, 2)
  assert.ok(kinds.lastIndexOf('gh release create') < pushAt)
  assert.ok(kinds.lastIndexOf('git tag -a') < kinds.indexOf('gh release view'))
  const push = calls[pushAt]
  assert.deepEqual(push.slice(1), [
    'push',
    '--atomic',
    'origin',
    'refs/tags/v0.51.0',
    'refs/tags/v0.51.1',
    'refs/tags/v0.52.0',
  ])
})

test('a failed draft means no push', () => {
  const calls: string[][] = []
  const failing = (a: string[]) => a[2] === 'view' || (a[2] === 'create' && a[3] === 'v0.51.0')
  assert.equal(
    applyPlan([plan('v0.51.0', true), plan('v0.52.0', true)], fakeRunner(calls, failing)),
    1,
  )
  assert.equal(
    calls.some((a) => a[1] === 'push'),
    false,
  )
})

test('existing releases are skipped', () => {
  const calls: string[][] = []
  assert.equal(applyPlan([plan('v0.51.0', true)], fakeRunner(calls)), 0)
  assert.equal(
    calls.some((a) => a[2] === 'create'),
    false,
  )
  assert.equal(
    calls.some((a) => a[1] === 'push'),
    true,
  )
})
