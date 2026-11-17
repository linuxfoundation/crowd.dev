import assert from 'node:assert/strict'
import { test } from 'node:test'

import { bumpFor, compareVersions, isReleaseType, isSemverTag, nextVersion } from './version.ts'

test('feat is minor', () =>
  assert.equal(nextVersion('v0.50.0', bumpFor('feat: x'), 'v0.1.0'), 'v0.51.0'))
test('fix is patch', () =>
  assert.equal(nextVersion('v0.50.3', bumpFor('fix(api): x'), 'v0.1.0'), 'v0.50.4'))
test('breaking on 0.x is minor', () =>
  assert.equal(nextVersion('v0.50.3', bumpFor('feat!: x'), 'v0.1.0'), 'v0.51.0'))
test('breaking on 1.x is major', () =>
  assert.equal(nextVersion('v1.2.3', bumpFor('refactor!: x'), 'v0.1.0'), 'v2.0.0'))
test('no previous tag uses initial', () =>
  assert.equal(nextVersion(null, 'minor', 'v0.50.0'), 'v0.50.0'))
test('non-conventional title is patch without release', () => {
  assert.equal(bumpFor('Revert "feat: x"'), 'patch')
  assert.equal(isReleaseType('Revert "feat: x"'), false)
})
test('chore is not a release type', () => assert.equal(isReleaseType('chore(deps): bump x'), false))
test('malformed latest tag throws', () =>
  assert.throws(() => nextVersion('nope', 'patch', 'v0.1.0')))

test('compareVersions orders numerically, not lexically', () => {
  assert.ok(compareVersions('v0.9.0', 'v0.10.0') < 0)
  assert.ok(compareVersions('v1.0.0', 'v0.99.99') > 0)
  assert.equal(compareVersions('v0.50.1', 'v0.50.1'), 0)
  assert.ok(compareVersions('v0.50.2', 'v0.50.10') < 0)
})
test('compareVersions throws on malformed tags', () =>
  assert.throws(() => compareVersions('v0.50.1-rc.1', 'v0.50.0'), /Malformed/))
test('isSemverTag is strict', () => {
  assert.equal(isSemverTag('v0.50.1'), true)
  assert.equal(isSemverTag('v0.50.1-rc.1'), false)
  assert.equal(isSemverTag('0.50.1'), false)
})
