import type { PackageRepoSignal } from '@crowd/data-access-layer/src/packages/repoConfidence'

import { CanonicalRepo, canonicalizeRepoUrl } from './canonicalizeRepoUrl'

export interface ManifestRepoCandidate {
  field: string
  url: string | null | undefined
  // Defaults to `primary` for the first candidate and `secondary` for the rest; set it
  // when the primary field was already resolved elsewhere.
  signal?: PackageRepoSignal
}

export interface ResolvedManifestRepo {
  repo: CanonicalRepo
  signal: PackageRepoSignal
}

export function resolveManifestRepo(
  candidates: ManifestRepoCandidate[],
): ResolvedManifestRepo | null {
  for (const [index, candidate] of candidates.entries()) {
    const raw = candidate.url?.trim()
    if (!raw) continue

    const repo = canonicalizeRepoUrl(raw)
    if (!repo) continue

    const signal: PackageRepoSignal = candidate.signal ?? (index === 0 ? 'primary' : 'secondary')
    if (signal === 'secondary' && repo.host === 'other') continue

    return { repo, signal }
  }
  return null
}
