import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { ghJson, run } from './gh.ts'
import { renderReleaseBody } from './release-body.ts'
import { loadRules, servicesForPaths } from './services.ts'
import type { Rule } from './services.ts'
import { bumpFor, compareVersions, isReleaseType, isSemverTag, nextVersion } from './version.ts'

export type CommitInfo = {
  sha: string
  title: string
  number: number | null
  author: string | null
  paths: string[]
  existingTag: string | null
}

export type PlannedTag = {
  sha: string
  version: string
  release: boolean
  title: string
  body: string
}

/** Plans a tag for every commit (oldest first) that has none, chaining versions across them. */
export function planTags(
  commits: CommitInfo[],
  latestTag: string | null,
  initial: string,
  rules: Rule[],
): PlannedTag[] {
  const planned: PlannedTag[] = []
  let current = latestTag
  for (const commit of commits) {
    if (commit.existingTag) {
      current = commit.existingTag
      continue
    }
    const version = nextVersion(current, bumpFor(commit.title), initial)
    current = version
    planned.push({
      sha: commit.sha,
      version,
      release: isReleaseType(commit.title),
      title: commit.title,
      body: renderReleaseBody({
        title: commit.title,
        number: commit.number,
        author: commit.author,
        services: servicesForPaths(commit.paths, rules),
      }),
    })
  }
  return planned
}

type PullInfo = {
  number: number
  title: string
  merged_at: string | null
  user: { login: string } | null
}

const lines = (out: string): string[] => out.split('\n').filter((l) => l !== '')

/** Strict vX.Y.Z tags per commit, from annotated (peeled) and lightweight tags. */
function semverTagsByCommit(cwd?: string): Map<string, string[]> {
  const out = run(
    'git',
    ['for-each-ref', '--format=%(refname:short) %(objectname) %(*objectname)', 'refs/tags/v*'],
    cwd,
  )
  const byCommit = new Map<string, string[]>()
  for (const line of lines(out)) {
    const [tag, object, peeled] = line.split(' ')
    if (!isSemverTag(tag)) continue
    const commit = peeled || object
    byCommit.set(commit, [...(byCommit.get(commit) ?? []), tag])
  }
  return byCommit
}

const highest = (tags: string[]): string =>
  tags.reduce((a, b) => (compareVersions(a, b) >= 0 ? a : b))

/** Highest strict vX.Y.Z tag on the first-parent chain of sha, or null when there is none. */
export function latestTagFor(sha: string, cwd?: string): string | null {
  const byCommit = semverTagsByCommit(cwd)
  const found = lines(run('git', ['rev-list', '--first-parent', sha], cwd)).flatMap(
    (c) => byCommit.get(c) ?? [],
  )
  return found.length === 0 ? null : highest(found)
}

/** A base tag older than the configured initial version is ignored, so history before it is never tagged. */
export function resolveBase(latest: string | null, initial: string): string | null {
  return latest !== null && compareVersions(latest, initial) < 0 ? null : latest
}

export const MAX_COMMITS = 50

/** First-parent commits after base up to sha, oldest first; just sha when there is no base. */
export function commitsSince(base: string | null, sha: string, cwd?: string): string[] {
  if (base === null) return [sha]
  const commits = lines(
    run('git', ['rev-list', '--first-parent', '--reverse', `${base}..${sha}`], cwd),
  )
  if (commits.length > MAX_COMMITS) {
    throw new Error(
      `${commits.length} commits since ${base} exceeds the limit of ${MAX_COMMITS}; tag a recent commit manually`,
    )
  }
  return commits
}

export function pathsOf(sha: string, cwd?: string): string[] {
  return lines(run('git', ['diff-tree', '--no-commit-id', '--name-only', '-r', sha], cwd))
}

/** Highest strict vX.Y.Z tag pointing at sha, or null. */
export function existingTagAt(sha: string, cwd?: string): string | null {
  const tags = lines(run('git', ['tag', '--points-at', sha, '--list', 'v*'], cwd)).filter(
    isSemverTag,
  )
  return tags.length === 0 ? null : highest(tags)
}

type Runner = (cmd: string, args: string[]) => string

/**
 * Local tags first, then draft releases, then one atomic push. A failed draft aborts before
 * the push, so a re-run replans the same versions and retries.
 */
export function applyPlan(planned: PlannedTag[], exec: Runner = run): number {
  for (const t of planned) exec('git', ['tag', '-a', t.version, '-m', t.title, t.sha])

  const dir = mkdtempSync(join(tmpdir(), 'release-notes-'))
  try {
    for (const t of planned.filter((p) => p.release)) {
      try {
        exec('gh', ['release', 'view', t.version])
        console.log(`release ${t.version} already exists`)
        continue
      } catch {
        // not found: create it below
      }
      try {
        const notes = join(dir, `${t.version}.md`)
        writeFileSync(notes, t.body)
        exec('gh', [
          'release',
          'create',
          t.version,
          '--draft',
          '--target',
          t.sha,
          '--title',
          `${t.version} — ${t.title}`,
          '--notes-file',
          notes,
        ])
      } catch (err) {
        console.log(
          `::error::could not create draft release ${t.version}: ${(err as Error).message}`,
        )
        return 1
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }

  exec('git', ['push', '--atomic', 'origin', ...planned.map((t) => `refs/tags/${t.version}`)])
  console.log(`tagged ${planned.map((t) => t.version).join(', ')}`)
  return 0
}

function describeCommit(sha: string, repo: string): CommitInfo {
  let title: string | null = null
  let number: number | null = null
  let author: string | null = null
  try {
    const pr = ghJson<PullInfo[]>(['api', `repos/${repo}/commits/${sha}/pulls`]).find(
      (p) => p.merged_at,
    )
    if (pr) {
      title = pr.title
      number = pr.number
      author = pr.user?.login ?? null
    }
  } catch (err) {
    console.log(`no pull request found for ${sha}: ${(err as Error).message}`)
  }
  title ??= run('git', ['log', '-1', '--format=%s', sha])
  return { sha, title, number, author, paths: pathsOf(sha), existingTag: existingTagAt(sha) }
}

export async function runTag(env: NodeJS.ProcessEnv): Promise<number> {
  const initial = env.INPUT_INITIAL_VERSION ?? ''
  if (initial === '') {
    console.error('error: initial-version is required for the tag command')
    return 1
  }
  const sha = env.GITHUB_SHA ?? ''
  const repo = env.GITHUB_REPOSITORY ?? ''
  if (sha === '' || repo === '') {
    console.error('error: GITHUB_SHA and GITHUB_REPOSITORY must be set')
    return 1
  }
  const rules = loadRules(env.INPUT_CONFIG || '.github/release-services.json')

  run('git', ['fetch', '--tags'])
  const latest = resolveBase(latestTagFor(sha), initial)
  const commits = commitsSince(latest, sha).map((s) => describeCommit(s, repo))
  const planned = planTags(commits, latest, initial, rules)
  if (planned.length === 0) {
    console.log('nothing to tag')
    return 0
  }

  run('git', ['config', 'user.name', 'github-actions[bot]'])
  run('git', ['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'])
  return applyPlan(planned)
}
