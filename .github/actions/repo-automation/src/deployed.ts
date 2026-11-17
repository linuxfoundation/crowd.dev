import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { run } from './gh.ts'
import { parseReleaseServices } from './release-body.ts'
import { compareVersions, isSemverTag } from './version.ts'

export type Draft = { tag: string; sha: string; createdAt: string; services: string[] | null }

const DAY_MS = 24 * 60 * 60 * 1000
const POLL_SECONDS = 15

export function selectDraftsToPublish(i: {
  drafts: Draft[]
  deployed: string[]
  onMain: boolean
  isAncestor: (sha: string) => boolean
  now: Date
  maxAgeDays: number
}): Draft[] {
  if (!i.onMain) return []
  return i.drafts
    .filter((d) => {
      if (!i.isAncestor(d.sha)) return false
      if (d.services === null) return true
      if (d.services.some((s) => i.deployed.includes(s))) return true
      return i.now.getTime() - new Date(d.createdAt).getTime() > i.maxAgeDays * DAY_MS
    })
    .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())
}

export type Runner = (cmd: string, args: string[]) => string

export type Deps = {
  exec: Runner
  now: () => Date
  sleep: (ms: number) => Promise<void>
}

const defaultDeps: Deps = {
  exec: (cmd, args) => run(cmd, args),
  now: () => new Date(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}

/** True when `sha` is reachable from origin/main. */
export function isOnMain(sha: string, exec: Runner = run): boolean {
  try {
    exec('git', ['merge-base', '--is-ancestor', sha, 'origin/main'])
    return true
  } catch {
    return false
  }
}

/** True when `sha` is contained in `deployedSha`. */
export function isAncestorOf(sha: string, deployedSha: string, exec: Runner = run): boolean {
  try {
    exec('git', ['merge-base', '--is-ancestor', sha, deployedSha])
    return true
  } catch {
    return false
  }
}

/** Commit a tag points at, or null when the tag does not exist locally. */
export function tagCommit(tag: string, exec: Runner = run): string | null {
  try {
    return exec('git', ['rev-parse', '--verify', '--quiet', `refs/tags/${tag}^{commit}`]) || null
  } catch {
    return null
  }
}

type ApiRelease = { tag_name: string; body: string | null; created_at: string }

function recordDeployments(
  exec: Runner,
  repo: string,
  sha: string,
  environment: string,
  services: string[],
): void {
  let version = sha
  try {
    version = exec('git', ['describe', '--tags', '--match', 'v[0-9]*', '--always', sha])
  } catch (err) {
    console.log(`::warning::could not describe ${sha}: ${msg(err)}`)
  }
  const dir = mkdtempSync(join(tmpdir(), 'deployment-'))
  try {
    for (const service of services) {
      try {
        const file = join(dir, `${service}.json`)
        writeFileSync(
          file,
          JSON.stringify({
            ref: sha,
            environment,
            task: `deploy:${service}`,
            auto_merge: false,
            required_contexts: [],
            payload: { service, version },
          }),
        )
        const created = JSON.parse(
          exec('gh', ['api', '-X', 'POST', `repos/${repo}/deployments`, '--input', file]),
        ) as { id: number }
        exec('gh', [
          'api',
          '-X',
          'POST',
          `repos/${repo}/deployments/${created.id}/statuses`,
          '-f',
          'state=success',
        ])
        console.log(`recorded deployment ${service} (${version}) in ${environment}`)
      } catch (err) {
        console.log(`::warning::could not record deployment for ${service}: ${msg(err)}`)
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

function msg(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function listDrafts(exec: Runner, repo: string): Draft[] {
  const out = exec('gh', [
    'api',
    `repos/${repo}/releases`,
    '--paginate',
    '--jq',
    '.[] | select(.draft == true) | {tag_name, body, created_at}',
  ])
  const drafts: Draft[] = []
  for (const line of out.split('\n').filter((l) => l.trim())) {
    const r = JSON.parse(line) as ApiRelease
    if (!isSemverTag(r.tag_name)) continue
    const sha = tagCommit(r.tag_name, exec)
    if (!sha) {
      console.log(`::warning::draft ${r.tag_name} has no tag yet, skipping`)
      continue
    }
    drafts.push({
      tag: r.tag_name,
      sha,
      createdAt: r.created_at,
      services: parseReleaseServices(r.body ?? ''),
    })
  }
  return drafts
}

function currentLatestTag(exec: Runner, repo: string): string | null {
  try {
    const r = JSON.parse(exec('gh', ['api', `repos/${repo}/releases/latest`])) as {
      tag_name?: string
    }
    return r.tag_name && isSemverTag(r.tag_name) ? r.tag_name : null
  } catch {
    return null
  }
}

export async function runDeployed(
  env: NodeJS.ProcessEnv,
  deps: Deps = defaultDeps,
): Promise<number> {
  const { exec, now, sleep } = deps
  const repo = env.GITHUB_REPOSITORY ?? ''
  const sha = env.GITHUB_SHA ?? ''
  if (!repo || !sha) throw new Error('GITHUB_REPOSITORY and GITHUB_SHA are required')
  const services = (env.INPUT_SERVICES ?? '').split(/\s+/).filter(Boolean)
  const deploymentEnv = (env.INPUT_DEPLOYMENT_ENVIRONMENT ?? '').trim()

  if (deploymentEnv) recordDeployments(exec, repo, sha, deploymentEnv, services)
  if (env.INPUT_PUBLISH !== 'true') return 0

  try {
    exec('git', ['fetch', 'origin', 'main', '--tags'])
    const onMain = isOnMain(sha, exec)
    if (!onMain) {
      console.log(`${sha} is not on main, not publishing releases`)
      return 0
    }

    const waitSeconds = Number(env.INPUT_TAG_WAIT_SECONDS ?? '0') || 0
    let waited = 0
    while (exec('git', ['tag', '--points-at', sha]) === '') {
      if (waited >= waitSeconds) {
        console.log(`::warning::no tag points at ${sha} after ${waited}s, continuing`)
        break
      }
      await sleep(POLL_SECONDS * 1000)
      waited += POLL_SECONDS
      exec('git', ['fetch', 'origin', 'main', '--tags'])
    }

    const drafts = listDrafts(exec, repo)
    const selected = selectDraftsToPublish({
      drafts,
      deployed: services,
      onMain,
      isAncestor: (s) => isAncestorOf(s, sha, exec),
      now: now(),
      maxAgeDays: 3,
    })
    if (selected.length === 0) {
      console.log('no draft releases to publish')
      return 0
    }

    const newest = selected.reduce((a, b) => (compareVersions(b.tag, a.tag) > 0 ? b : a)).tag
    const existing = currentLatestTag(exec, repo)
    const markLatest = existing === null || compareVersions(existing, newest) <= 0

    for (const d of selected) {
      try {
        const latest = markLatest && d.tag === newest ? '--latest' : '--latest=false'
        exec('gh', ['release', 'edit', d.tag, '--draft=false', latest])
        console.log(`published ${d.tag}`)
      } catch (err) {
        console.log(`::warning::could not publish ${d.tag}: ${msg(err)}`)
      }
    }
  } catch (err) {
    console.log(`::warning::release publishing failed: ${msg(err)}`)
  }
  return 0
}
