import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, before, test } from 'node:test'

import { commitsSince, existingTagAt, latestTagFor, MAX_COMMITS } from './tag.ts'

process.env.GIT_CONFIG_GLOBAL = '/dev/null'
process.env.GIT_CONFIG_NOSYSTEM = '1'

let dir = ''
const git = (...args: string[]): string =>
  execFileSync(
    'git',
    [
      '-c',
      'user.name=t',
      '-c',
      'user.email=t@t',
      '-c',
      'commit.gpgsign=false',
      '-c',
      'tag.gpgsign=false',
      ...args,
    ],
    { cwd: dir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  ).trim()
const commit = (msg: string): string => {
  writeFileSync(join(dir, 'f'), msg)
  git('add', '.')
  git('commit', '-q', '-m', msg)
  return git('rev-parse', 'HEAD')
}

let head = ''
let multi = ''

before(() => {
  dir = mkdtempSync(join(tmpdir(), 'tag-git-'))
  git('init', '-q', '-b', 'main')
  commit('c1')
  git('tag', '-a', 'v0.44.0', '-m', 'x')
  git('checkout', '-q', '-b', 'side')
  commit('side')
  git('tag', '-a', 'v0.49.0', '-m', 'x')
  git('checkout', '-q', 'main')
  git('merge', '-q', '--no-ff', '-m', 'merge side', 'side')
  multi = commit('c3')
  git('tag', '-a', 'v0.9.0', '-m', 'x')
  git('tag', 'v0.10.0')
  git('tag', 'v0.10.0-rc.1')
  head = commit('c4')
  git('tag', 'v0.50.1-rc.1')
})

after(() => rmSync(dir, { recursive: true, force: true }))

test('base is picked on the first-parent chain, ignoring side-branch and non-semver tags', () =>
  assert.equal(latestTagFor(head, dir), 'v0.44.0'))

test('the highest of several tags wins; non-semver tags are ignored', () => {
  assert.equal(existingTagAt(multi, dir), 'v0.10.0')
  assert.equal(existingTagAt(head, dir), null)
})

test('commitsSince lists first-parent commits oldest first', () =>
  assert.equal(commitsSince('v0.44.0', head, dir).length, 3))

test('a null base plans only the sha', () =>
  assert.deepEqual(commitsSince(null, head, dir), [head]))

test('more than the cap fails with the base and count', () => {
  for (let i = 0; i < MAX_COMMITS; i++) commit(`bulk ${i}`)
  const tip = git('rev-parse', 'HEAD')
  assert.throws(() => commitsSince('v0.44.0', tip, dir), /v0\.44\.0.*exceeds the limit/)
})
