import { Extractor, ExtractorResult, RawContact, RepoPolicies } from '../../types'

import { fetchCargo } from './cargo'
import { fetchComposer } from './composer'
import { fetchMaven } from './maven'
import { fetchNpm } from './npm'
import { fetchNuget } from './nuget'
import { ParsedPurl, parsePurl } from './purl'
import { fetchPypi } from './pypi'
import { fetchRubygems } from './rubygems'

type EcosystemFetcher = (
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
  repoUrl?: string,
) => Promise<ExtractorResult>

// Keyed by the lowercased packages.ecosystem value. go has no package-manifest contacts.
const FETCHERS: Record<string, EcosystemFetcher> = {
  npm: fetchNpm,
  pypi: fetchPypi,
  maven: fetchMaven,
  cargo: fetchCargo,
  nuget: fetchNuget,
  rubygems: fetchRubygems,
  composer: fetchComposer,
}

export const extractManifest: Extractor = async (target, deps) => {
  const contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  const candidatesByHandle = new Map<string, RawContact>()
  const seenPurls = new Set<string>()

  for (const pkg of target.packages) {
    if (seenPurls.has(pkg.purl)) continue
    seenPurls.add(pkg.purl)

    const fetcher = FETCHERS[pkg.ecosystem?.toLowerCase()]
    if (!fetcher) continue

    const parsed = parsePurl(pkg.purl)
    if (!parsed) continue

    try {
      const result = await fetcher(parsed, deps.fetchTimeoutMs, deps.userAgent, target.url)
      contacts.push(...result.contacts)
      for (const candidate of result.handleCandidates ?? []) {
        const key = candidate.value.toLowerCase()
        const existing = candidatesByHandle.get(key)
        if (existing) {
          existing.provenance.push(...candidate.provenance)
        } else {
          candidatesByHandle.set(key, { ...candidate, provenance: [...candidate.provenance] })
        }
      }
      for (const [key, value] of Object.entries(result.policies)) {
        if (!(policies as Record<string, unknown>)[key] && value != null) {
          ;(policies as Record<string, unknown>)[key] = value
        }
      }
    } catch {
      // one bad package must not sink the rest
      continue
    }
  }

  return { contacts, policies, handleCandidates: [...candidatesByHandle.values()] }
}
