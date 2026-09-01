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
  field: string
}

/**
 * Resolves a package's repository from the manifest fields that may carry it, in
 * declaration order: the first candidate is the ecosystem's canonical repository
 * field (`primary`), every later one a fallback (`secondary`).
 *
 * Fallback fields are free-form (homepage, docs, bug tracker), so they are only
 * accepted on a recognized VCS host — an arbitrary `https://example.com/a/b`
 * canonicalizes fine but is not a repo. The primary field keeps its historical
 * behavior and accepts `other` hosts (self-hosted Gitea, cgit, SVN).
 */
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

    return { repo, signal, field: candidate.field }
  }
  return null
}
