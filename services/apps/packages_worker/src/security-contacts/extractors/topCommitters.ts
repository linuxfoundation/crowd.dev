import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import { ExtractorDeps, ProvenanceEntry, RawContact, RepoTarget } from '../types'

import { isEmail } from './http'

const log = getServiceChildLogger('security-contacts:top-committers')

const SOURCE = 'github-commits'
const SINCE_DAYS = 90
const TOP_N = 3
const PER_PAGE = 100
const MAX_PAGES = 3

interface CommitAuthor {
  email?: unknown
  name?: unknown
  date?: unknown
}

interface CommitEntry {
  commit?: { author?: CommitAuthor }
  author?: { login?: unknown; type?: unknown } | null
}

interface AggregatedAuthor {
  email: string
  name?: string
  login?: string
  count: number
  latestDate: string
}

const BOT_TOKENS = ['dependabot', 'renovate', 'github-actions']

export function isBotCommit(c: CommitEntry): boolean {
  if (c.author?.type === 'Bot') return true
  const email = typeof c.commit?.author?.email === 'string' ? c.commit.author.email : ''
  const name = typeof c.commit?.author?.name === 'string' ? c.commit.author.name : ''
  const login = typeof c.author?.login === 'string' ? c.author.login : ''
  if (/\[bot\]@/i.test(email)) return true
  const haystack = `${email} ${name} ${login}`.toLowerCase()
  return BOT_TOKENS.some((token) => haystack.includes(token))
}

export function aggregateTopCommitters(
  commits: CommitEntry[],
  topN: number = TOP_N,
): AggregatedAuthor[] {
  const byEmail = new Map<string, AggregatedAuthor>()

  for (const c of commits) {
    if (isBotCommit(c)) continue

    const rawEmail = c.commit?.author?.email
    if (typeof rawEmail !== 'string' || !isEmail(rawEmail)) continue
    const email = rawEmail.toLowerCase()

    const rawName = c.commit?.author?.name
    const name = typeof rawName === 'string' ? rawName : undefined
    const rawDate = c.commit?.author?.date
    const date = typeof rawDate === 'string' ? rawDate : undefined
    const rawLogin = c.author?.login
    const login = typeof rawLogin === 'string' ? rawLogin : undefined

    const existing = byEmail.get(email)
    if (existing) {
      existing.count += 1
      if (!existing.login && login) existing.login = login
      if (date && date > existing.latestDate) existing.latestDate = date
    } else {
      byEmail.set(email, {
        email,
        name,
        login,
        count: 1,
        latestDate: date ?? new Date(0).toISOString(),
      })
    }
  }

  return [...byEmail.values()]
    .sort((a, b) => b.count - a.count || a.email.localeCompare(b.email))
    .slice(0, topN)
}

export function mapCommittersToContacts(
  authors: AggregatedAuthor[],
  fetchedAt: string,
  apiPath: string,
): RawContact[] {
  const contacts: RawContact[] = []

  for (const author of authors) {
    const provenance: ProvenanceEntry[] = [
      {
        source: SOURCE,
        sourceTier: 'D',
        path: apiPath,
        fetchedAt,
        declaredAt: author.latestDate,
      },
    ]

    contacts.push({
      channel: 'email',
      value: author.email,
      name: author.name,
      handle: author.login,
      role: 'committer',
      tier: 'D',
      provenance,
    })

    if (author.login) {
      contacts.push({
        channel: 'github-handle',
        value: author.login,
        handle: author.login,
        role: 'committer',
        tier: 'D',
        provenance,
      })
    }
  }

  return contacts
}

async function fetchCommitPages(
  owner: string,
  name: string,
  since: string,
  deps: Pick<ExtractorDeps, 'githubGet'>,
): Promise<CommitEntry[]> {
  const commits: CommitEntry[] = []

  for (let page = 1; page <= MAX_PAGES; page++) {
    const path = `/repos/${owner}/${name}/commits?since=${since}&per_page=${PER_PAGE}&page=${page}`
    const { text } = await deps.githubGet(path)
    if (!text) break

    const entries = JSON.parse(text) as CommitEntry[]
    if (!Array.isArray(entries) || entries.length === 0) break

    commits.push(...entries)
    if (entries.length < PER_PAGE) break
  }

  return commits
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

  const since = new Date(now().getTime() - SINCE_DAYS * 24 * 60 * 60 * 1000).toISOString()
  const apiPath = `https://api.github.com/repos/${owner}/${name}/commits?since=${since}`

  let commits: CommitEntry[]
  try {
    commits = await fetchCommitPages(owner, name, since, deps)
  } catch (err) {
    log.warn(
      { repoId: target.repoId, owner, name, errMsg: (err as Error).message },
      'Commit lookup failed',
    )
    return []
  }

  const authors = aggregateTopCommitters(commits)
  const fetchedAt = new Date().toISOString()
  return mapCommittersToContacts(authors, fetchedAt, apiPath)
}
