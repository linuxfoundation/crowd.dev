import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, before, test } from 'node:test'

import { isAncestorOf, runDeployed, selectDraftsToPublish, tagCommit } from './deployed.ts'
import type { Deps } from './deployed.ts'
import { renderReleaseBody } from './release-body.ts'

process.env.GIT_CONFIG_GLOBAL = '/dev/null'
process.env.GIT_CONFIG_NOSYSTEM = '1'

const now = new Date('2026-11-20T00:00:00Z')
const d = (tag: string, services: string[] | null, createdAt = '2026-11-19T00:00:00Z') => ({
  tag,
  sha: tag,
  createdAt,
  services,
})
const base = { deployed: ['api'], onMain: true, isAncestor: () => true, now, maxAgeDays: 3 }

test('publishes overlapping and unknown, skips others', () =>
  assert.deepEqual(
    selectDraftsToPublish({
      ...base,
      drafts: [d('v1', ['api']), d('v2', ['worker']), d('v3', null)],
    }).map((x) => x.tag),
    ['v1', 'v3'],
  ))
test('publishes drafts older than 3 days regardless of service', () =>
  assert.deepEqual(
    selectDraftsToPublish({ ...base, drafts: [d('v1', ['worker'], '2026-11-16T00:00:00Z')] }).map(
      (x) => x.tag,
    ),
    ['v1'],
  ))
test('publishes nothing when sha is not on main', () =>
  assert.deepEqual(
    selectDraftsToPublish({ ...base, onMain: false, drafts: [d('v1', ['api'])] }),
    [],
  ))
test('ignores drafts not contained in the deployed sha', () =>
  assert.deepEqual(
    selectDraftsToPublish({
      ...base,
      isAncestor: (s) => s !== 'v2',
      drafts: [d('v1', ['api']), d('v2', ['api'])],
    }).map((x) => x.tag),
    ['v1'],
  ))
test('own draft missing still publishes older eligible drafts', () =>
  assert.deepEqual(
    selectDraftsToPublish({ ...base, drafts: [d('v1', ['api'])] }).map((x) => x.tag),
    ['v1'],
  ))
test('returns oldest first', () =>
  assert.deepEqual(
    selectDraftsToPublish({
      ...base,
      drafts: [d('v2', ['api'], '2026-11-19T10:00:00Z'), d('v1', ['api'], '2026-11-19T01:00:00Z')],
    }).map((x) => x.tag),
    ['v1', 'v2'],
  ))

// --- orchestration with a fake runner ---

type Call = { cmd: string; args: string[] }

function fake(opts: {
  onMain?: boolean
  drafts?: { tag_name: string; body: string; created_at: string }[]
  tagCommits?: Record<string, string>
  latest?: string | null
  failDeployments?: boolean
  tagsAtSha?: string[]
}) {
  const calls: Call[] = []
  const sleeps: number[] = []
  const warnings: string[] = []
  const infos: string[] = []
  const tagsAt = [...(opts.tagsAtSha ?? ['v9.9.9'])]
  const exec = (cmd: string, args: string[]): string => {
    calls.push({ cmd, args })
    const a = args.join(' ')
    if (cmd === 'git') {
      if (a.startsWith('merge-base --is-ancestor') && a.endsWith('origin/main')) {
        if (opts.onMain === false) throw new Error('not ancestor')
        return ''
      }
      if (a.startsWith('merge-base --is-ancestor')) return ''
      if (a.startsWith('describe')) return 'v1.2.3-4-gabc'
      if (a.startsWith('tag --points-at')) return tagsAt.shift() ?? ''
      if (a.startsWith('rev-parse')) {
        const tag = /refs\/tags\/(.+)\^\{commit\}/.exec(a)?.[1] ?? ''
        const sha = opts.tagCommits?.[tag]
        if (!sha) throw new Error('no tag')
        return sha
      }
      return ''
    }
    if (a.includes('/deployments/') && a.includes('statuses')) return '{}'
    if (a.includes('/deployments')) {
      if (opts.failDeployments) throw new Error('boom')
      return '{"id": 7}'
    }
    if (a.includes('releases/latest')) {
      if (!opts.latest) throw new Error('404')
      return JSON.stringify({ tag_name: opts.latest })
    }
    if (a.includes('/releases')) return (opts.drafts ?? []).map((x) => JSON.stringify(x)).join('\n')
    return ''
  }
  const deps: Deps = {
    exec,
    now: () => now,
    sleep: async (ms) => {
      sleeps.push(ms)
    },
    log: {
      info: (m) => void infos.push(m),
      warning: (m) => void warnings.push(m),
      error: (m) => void warnings.push(`error: ${m}`),
    },
  }
  const edits = () => calls.filter((c) => c.cmd === 'gh' && c.args[0] === 'release')
  return { calls, sleeps, warnings, infos, deps, edits }
}

const env = (extra: Record<string, string> = {}): NodeJS.ProcessEnv => ({
  GITHUB_REPOSITORY: 'o/r',
  GITHUB_SHA: 'deadbeef',
  INPUT_SERVICES: ' api  worker ',
  INPUT_PUBLISH: 'true',
  INPUT_TAG_WAIT_SECONDS: '60',
  ...extra,
})
const draft = (tag: string, services: string[] | null, created = '2026-11-19T00:00:00Z') => ({
  tag_name: tag,
  body: renderReleaseBody({ title: 't', number: 1, author: 'a', services }),
  created_at: created,
})

test('not on main: records deployments, never edits releases', async () => {
  const f = fake({ onMain: false, drafts: [draft('v1.0.0', ['api'])] })
  const code = await runDeployed(env({ INPUT_DEPLOYMENT_ENVIRONMENT: 'production' }), f.deps)
  assert.equal(code, 0)
  const posts = f.calls.filter((c) => c.cmd === 'gh' && c.args.join(' ').includes('/deployments'))
  assert.equal(posts.length, 4) // 2 services x (deployment + status)
  assert.equal(f.edits().length, 0)
  assert.deepEqual(f.warnings, [])
})

test('publishes oldest first and flags only the newest as latest', async () => {
  const f = fake({
    drafts: [
      draft('v1.2.0', ['api'], '2026-11-19T02:00:00Z'),
      draft('v1.1.0', ['api'], '2026-11-19T01:00:00Z'),
    ],
    tagCommits: { 'v1.1.0': 'a', 'v1.2.0': 'b' },
  })
  await runDeployed(env(), f.deps)
  assert.deepEqual(
    f.edits().map((c) => c.args),
    [
      ['release', 'edit', 'v1.1.0', '--draft=false', '--latest=false'],
      ['release', 'edit', 'v1.2.0', '--draft=false', '--latest'],
    ],
  )
})

test('never marks latest when a higher release is already published', async () => {
  const f = fake({
    latest: 'v2.0.0',
    drafts: [draft('v1.1.0', ['api'])],
    tagCommits: { 'v1.1.0': 'a' },
  })
  await runDeployed(env(), f.deps)
  assert.deepEqual(
    f.edits().map((c) => c.args),
    [['release', 'edit', 'v1.1.0', '--draft=false', '--latest=false']],
  )
})

test('skips orphan drafts whose tag does not exist and non-semver drafts', async () => {
  const f = fake({
    drafts: [draft('v1.1.0', ['api']), draft('v1.2.0', ['api']), draft('v1.3.0-rc.1', ['api'])],
    tagCommits: { 'v1.1.0': 'a' },
  })
  await runDeployed(env(), f.deps)
  assert.deepEqual(
    f.edits().map((c) => c.args[2]),
    ['v1.1.0'],
  )
  assert.equal(f.warnings.length, 1)
  assert.match(f.warnings[0], /draft v1\.2\.0 has no tag yet/)
})

test('deployment failure does not stop publishing', async () => {
  const f = fake({
    failDeployments: true,
    drafts: [draft('v1.1.0', ['api'])],
    tagCommits: { 'v1.1.0': 'a' },
  })
  const code = await runDeployed(env({ INPUT_DEPLOYMENT_ENVIRONMENT: 'production' }), f.deps)
  assert.equal(code, 0)
  assert.equal(f.edits().length, 1)
  assert.equal(f.warnings.length, 2)
  assert.match(f.warnings[0], /could not record deployment for api: boom/)
  assert.match(f.warnings[1], /could not record deployment for worker/)
})

test('publish failure is a warning, exit 0', async () => {
  const f = fake({ drafts: [draft('v1.1.0', ['api'])], tagCommits: { 'v1.1.0': 'a' } })
  const exec = f.deps.exec
  f.deps.exec = (cmd, args) => {
    if (cmd === 'gh' && args[0] === 'release') throw new Error('nope')
    return exec(cmd, args)
  }
  assert.equal(await runDeployed(env(), f.deps), 0)
  assert.equal(f.warnings.length, 1)
  assert.match(f.warnings[0], /could not publish v1\.1\.0: nope/)
})

test('builders-dir maps input services to builder names for publishing', async () => {
  const bdir = mkdtempSync(join(tmpdir(), 'deployed-builders-'))
  try {
    writeFileSync(join(bdir, 'backend.env'), 'SERVICES="api api-e2e"\n')
    const f = fake({ drafts: [draft('v1.1.0', ['backend'])], tagCommits: { 'v1.1.0': 'a' } })
    await runDeployed(
      env({
        INPUT_SERVICES: 'api mystery',
        INPUT_BUILDERS_DIR: bdir,
        INPUT_DEPLOYMENT_ENVIRONMENT: 'production',
      }),
      f.deps,
    )
    assert.deepEqual(
      f.edits().map((c) => c.args[2]),
      ['v1.1.0'],
    )
    assert.deepEqual(f.warnings, ['no builder found for service mystery'])
    const tasks = f.calls
      .filter((c) => c.cmd === 'gh' && c.args.includes('--input'))
      .map((c) => c.args.join(' '))
    assert.equal(tasks.length, 2) // deployments recorded per input name (api, mystery)
    const withoutMap = fake({
      drafts: [draft('v1.1.0', ['backend'])],
      tagCommits: { 'v1.1.0': 'a' },
    })
    await runDeployed(env({ INPUT_SERVICES: 'api' }), withoutMap.deps)
    assert.equal(withoutMap.edits().length, 0)
  } finally {
    rmSync(bdir, { recursive: true, force: true })
  }
})

test('publish false returns before touching git', async () => {
  const f = fake({})
  assert.equal(await runDeployed(env({ INPUT_PUBLISH: 'false' }), f.deps), 0)
  assert.equal(f.calls.length, 0)
})

test('waits for the tag with the injected sleep, then gives up at the timeout', async () => {
  const f = fake({ tagsAtSha: ['', '', ''], drafts: [] })
  await runDeployed(env({ INPUT_TAG_WAIT_SECONDS: '30' }), f.deps)
  assert.deepEqual(f.sleeps, [15000, 15000])
  assert.equal(f.warnings.length, 1)
  assert.match(f.warnings[0], /no tag points at deadbeef after 30s/)
})

// --- hermetic git helpers ---

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
const exec = (cmd: string, args: string[]): string => {
  assert.equal(cmd, 'git')
  return git(...args)
}
let c1 = ''
let c2 = ''
let side = ''

before(() => {
  dir = mkdtempSync(join(tmpdir(), 'deployed-git-'))
  git('init', '-q', '-b', 'main')
  const commit = (m: string): string => {
    writeFileSync(join(dir, 'f'), m)
    git('add', '.')
    git('commit', '-q', '-m', m)
    return git('rev-parse', 'HEAD')
  }
  c1 = commit('c1')
  git('tag', '-a', 'v0.1.0', '-m', 'x')
  c2 = commit('c2')
  git('checkout', '-q', '-b', 'side')
  side = commit('side')
  git('tag', 'v0.2.0')
  git('checkout', '-q', 'main')
})
after(() => rmSync(dir, { recursive: true, force: true }))

test('tagCommit resolves annotated and lightweight tags, null for orphans', () => {
  assert.equal(tagCommit('v0.1.0', exec), c1)
  assert.equal(tagCommit('v0.2.0', exec), side)
  assert.equal(tagCommit('v9.9.9', exec), null)
})

test('isAncestorOf tells contained commits from unrelated ones', () => {
  assert.equal(isAncestorOf(c1, c2, exec), true)
  assert.equal(isAncestorOf(side, c2, exec), false)
})
