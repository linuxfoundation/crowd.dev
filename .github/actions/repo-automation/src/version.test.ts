import assert from 'node:assert/strict'
import { test } from 'node:test'

import { bumpFor, isReleaseType, nextVersion } from './version.ts'

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
