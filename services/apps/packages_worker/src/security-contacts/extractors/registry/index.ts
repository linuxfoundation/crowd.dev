import { Extractor, RawContact, RepoPackage, RepoPolicies } from '../../types'

import { fetchNpm } from './npm'
import { parsePurl } from './purl'

type EcosystemFetcher = (
  parsed: ReturnType<typeof parsePurl> & object,
  pkg: RepoPackage,
  timeoutMs: number,
) => Promise<RawContact[]>

// Keyed by the lowercased packages.ecosystem value. go has no manifest contacts.
const FETCHERS: Record<string, EcosystemFetcher> = {
  npm: fetchNpm,
}

export const extractManifest: Extractor = async (target, deps) => {
  const contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  const seenPurls = new Set<string>()

  for (const pkg of target.packages) {
    if (seenPurls.has(pkg.purl)) continue
    seenPurls.add(pkg.purl)

    const fetcher = FETCHERS[pkg.ecosystem?.toLowerCase()]
    if (!fetcher) continue

    const parsed = parsePurl(pkg.purl)
    if (!parsed) continue

    try {
      contacts.push(...(await fetcher(parsed, pkg, deps.fetchTimeoutMs)))
    } catch {
      // one bad package must not sink the rest
      continue
    }
  }

  return { contacts, policies }
}
