import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../enricher/fetchLightRepo'

import { ExtractorDeps, RawContact, RepoTarget } from './types'

const log = getServiceChildLogger('security-contacts')

interface ContributorEntry {
  login?: unknown
}

/**
 * Corroborates registry usernames that are only *guessed* to be GitHub logins (e.g. RubyGems
 * owners). A candidate is confirmed only when the same login owns the repo or appears in its
 * top-100 contributors — existence of a github.com/<handle> account alone is not enough, since
 * an unrelated person or bot can hold that name.
 */
export async function verifyHandleCandidates(
  target: RepoTarget,
  deps: Pick<ExtractorDeps, 'githubGet'>,
  candidates: RawContact[],
): Promise<RawContact[]> {
  if (candidates.length === 0) return []

  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return []
  }

  const path = `/repos/${owner}/${name}/contributors?per_page=100`
  const corroborated = new Set<string>([owner.toLowerCase()])
  try {
    const { text } = await deps.githubGet(path)
    if (text) {
      const entries = JSON.parse(text) as ContributorEntry[]
      if (Array.isArray(entries)) {
        for (const e of entries) {
          if (typeof e.login === 'string') corroborated.add(e.login.toLowerCase())
        }
      }
    }
  } catch (err) {
    log.warn(
      { repoId: target.repoId, errMsg: (err as Error).message },
      'Contributor lookup failed — dropping handle candidates',
    )
    return []
  }

  const fetchedAt = new Date().toISOString()
  return candidates
    .filter((c) => corroborated.has(c.value.toLowerCase()))
    .map((c) => ({
      ...c,
      provenance: [
        ...c.provenance,
        {
          source: 'github-contributors',
          sourceTier: c.tier,
          path: `https://api.github.com${path}`,
          fetchedAt,
        },
      ],
    }))
}
