import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { ExtractorDeps, ProvenanceEntry, RawContact, RepoTarget } from '../types'

import { isEmail } from './http'

const log = getServiceChildLogger('security-contacts:top-committers')

const SOURCE = 'github-commits'
const SINCE_DAYS = 90
const TOP_N = 3

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

const BOT_TOKENS = ['dependabot', 'renovate', 'github-actions']

function isBotLogin(login: string): boolean {
  const lower = login.toLowerCase()
  if (lower.endsWith('[bot]')) return true
  return BOT_TOKENS.some((token) => lower.includes(token))
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
    return typeof email === 'string' && isEmail(email) ? email : null
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

export async function fetchTopCommitters(
  target: RepoTarget,
  deps: Pick<ExtractorDeps, 'githubGet'>,
  now: () => Date = () => new Date(),
): Promise<RawContact[]> {
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return []
  }

  const path = `/repos/${owner}/${name}/stats/contributors`
  let stats: ContributorStat[]
  try {
    const { text } = await deps.githubGet(path)
    if (!text) return []
    const parsed = JSON.parse(text)
    if (!Array.isArray(parsed)) return []
    stats = parsed as ContributorStat[]
  } catch (err) {
    log.warn(
      { repoId: target.repoId, owner, name, errMsg: (err as Error).message },
      'Contributor stats lookup failed',
    )
    return []
  }

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
