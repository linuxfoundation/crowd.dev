import { ghJson, run } from './gh.ts'
import { renderReleaseBody } from './release-body.ts'
import { loadRules, servicesForPaths } from './services.ts'
import type { Rule } from './services.ts'
import { bumpFor, isReleaseType, nextVersion } from './version.ts'

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

/** Latest vX.Y.Z tag reachable from sha, or null when there is none. */
export function latestTagFor(sha: string): string | null {
  try {
    return run('git', ['describe', '--tags', '--abbrev=0', '--match', 'v[0-9]*.[0-9]*.[0-9]*', sha])
  } catch {
    return null
  }
}

/** First-parent commits after latest up to sha, oldest first; just sha when there is no latest tag. */
export function commitsSince(latest: string | null, sha: string): string[] {
  if (latest === null) return [sha]
  return lines(run('git', ['rev-list', '--first-parent', '--reverse', `${latest}..${sha}`]))
}

export function pathsOf(sha: string): string[] {
  return lines(run('git', ['diff-tree', '--no-commit-id', '--name-only', '-r', sha]))
}

export function existingTagAt(sha: string): string | null {
  return lines(run('git', ['tag', '--points-at', sha, '--list', 'v*']))[0] ?? null
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
  const latest = latestTagFor(sha)
  const commits = commitsSince(latest, sha).map((s) => describeCommit(s, repo))
  const planned = planTags(commits, latest, initial, rules)
  if (planned.length === 0) {
    console.log('nothing to tag')
    return 0
  }

  run('git', ['config', 'user.name', 'github-actions[bot]'])
  run('git', ['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'])
  for (const t of planned) run('git', ['tag', '-a', t.version, '-m', t.title, t.sha])
  run('git', ['push', '--atomic', 'origin', ...planned.map((t) => `refs/tags/${t.version}`)])
  console.log(`tagged ${planned.map((t) => t.version).join(', ')}`)

  let failed = false
  for (const t of planned.filter((p) => p.release)) {
    try {
      run('gh', [
        'release',
        'create',
        t.version,
        '--draft',
        '--title',
        `${t.version} — ${t.title}`,
        '--notes',
        t.body,
        '--verify-tag',
      ])
    } catch (err) {
      failed = true
      console.log(
        `::warning::could not create draft release ${t.version}: ${(err as Error).message}`,
      )
    }
  }
  return failed ? 1 : 0
}
