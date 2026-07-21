import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { ExtractorDeps, ProvenanceEntry, RawContact, RepoTarget } from '../types'

import { isEmail } from './http'

const log = getServiceChildLogger('security-contacts:top-committers')

const SOURCE = 'github-commits'
const SINCE_DAYS = 90
const TOP_N = 3
// GitHub computes /stats/contributors asynchronously and returns 202 while it's not ready yet —
// poll a few times with a short backoff before giving up for this pass.
const STATS_POLL_ATTEMPTS = 3
const STATS_POLL_DELAY_MS = 2000

interface WeekStat {
  w?: unknown
  c?: unknown
}

interface ContributorStat {
  author?: { login?: unknown; type?: unknown } | null
  weeks?: WeekStat[]
}

interface AggregatedAuthor {
  login: string
  count: number
}

const BOT_TOKENS = [
  'dependabot',
  'renovate',
  'github-actions',
  'claude',
  'anthropic',
  'copilot',
  'chatgpt',
  'openai',
]

function matchesBotToken(value: string): boolean {
  const lower = value.toLowerCase()
  return BOT_TOKENS.some((token) => lower.includes(token))
}

function isBotLogin(login: string): boolean {
  const lower = login.toLowerCase()
  if (lower.endsWith('[bot]')) return true
  return matchesBotToken(lower)
}

export function aggregateTopCommitters(
  stats: ContributorStat[],
  sinceUnixSeconds: number,
  topN: number = TOP_N,
): AggregatedAuthor[] {
  const authors: AggregatedAuthor[] = []

  for (const stat of stats) {
    if (stat.author?.type === 'Bot') continue
    const login = typeof stat.author?.login === 'string' ? stat.author.login : undefined
    if (!login || isBotLogin(login)) continue

    const count = (stat.weeks ?? []).reduce((sum, week) => {
      const w = typeof week.w === 'number' ? week.w : 0
      const c = typeof week.c === 'number' ? week.c : 0
      return w >= sinceUnixSeconds ? sum + c : sum
    }, 0)
    if (count > 0) authors.push({ login, count })
  }

  return authors.sort((a, b) => b.count - a.count || a.login.localeCompare(b.login)).slice(0, topN)
}

async function resolvePublicEmail(
  login: string,
  githubGet: ExtractorDeps['githubGet'],
): Promise<string | null> {
  try {
    const { text } = await githubGet(`/users/${login}`)
    const email = (text ? (JSON.parse(text) as { email?: unknown }) : null)?.email
    if (typeof email !== 'string' || !isEmail(email) || matchesBotToken(email)) return null
    return email
  } catch (err) {
    log.warn({ login, errMsg: (err as Error).message }, 'Committer email resolution failed')
    return null
  }
}

export async function mapCommittersToContacts(
  authors: AggregatedAuthor[],
  fetchedAt: string,
  apiPath: string,
  githubGet: ExtractorDeps['githubGet'],
): Promise<RawContact[]> {
  const contacts: RawContact[] = []

  for (const author of authors) {
    const provenance: ProvenanceEntry[] = [
      { source: SOURCE, sourceTier: 'D', path: apiPath, fetchedAt },
    ]

    contacts.push({
      channel: 'github-handle',
      value: author.login,
      handle: author.login,
      role: 'committer',
      tier: 'D',
      provenance,
    })

    const email = await resolvePublicEmail(author.login, githubGet)
    if (email) {
      contacts.push({
        channel: 'email',
        value: email,
        handle: author.login,
        role: 'committer',
        tier: 'D',
        provenance,
      })
    }
  }

  return contacts
}

async function fetchContributorStats(
  path: string,
  target: RepoTarget,
  owner: string,
  name: string,
  githubGet: ExtractorDeps['githubGet'],
  sleep: (ms: number) => Promise<void>,
): Promise<ContributorStat[] | null> {
  for (let attempt = 1; attempt <= STATS_POLL_ATTEMPTS; attempt++) {
    let status: number
    let text: string | null
    try {
      ;({ status, text } = await githubGet(path, { extraOkStatuses: [202] }))
    } catch (err) {
      log.warn(
        { repoId: target.repoId, owner, name, errMsg: (err as Error).message },
        'Contributor stats lookup failed',
      )
      return null
    }

    if (status === 202) {
      if (attempt < STATS_POLL_ATTEMPTS) await sleep(STATS_POLL_DELAY_MS)
      continue
    }

    if (!text) return null
    try {
      const parsed = JSON.parse(text)
      return Array.isArray(parsed) ? (parsed as ContributorStat[]) : null
    } catch (err) {
      log.warn(
        { repoId: target.repoId, owner, name, errMsg: (err as Error).message },
        'Contributor stats lookup failed',
      )
      return null
    }
  }

  log.info(
    { repoId: target.repoId, owner, name },
    'Contributor stats still computing after polling — skipping this pass',
  )
  return null
}

export async function fetchTopCommitters(
  target: RepoTarget,
  deps: Pick<ExtractorDeps, 'githubGet'>,
  now: () => Date = () => new Date(),
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
): Promise<RawContact[]> {
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return []
  }

  const path = `/repos/${owner}/${name}/stats/contributors`
  const stats = await fetchContributorStats(path, target, owner, name, deps.githubGet, sleep)
  if (!stats) return []

  const sinceUnixSeconds = Math.floor(now().getTime() / 1000) - SINCE_DAYS * 24 * 60 * 60
  const authors = aggregateTopCommitters(stats, sinceUnixSeconds)
  const fetchedAt = new Date().toISOString()
  return mapCommittersToContacts(
    authors,
    fetchedAt,
    `https://api.github.com${path}`,
    deps.githubGet,
  )
}
