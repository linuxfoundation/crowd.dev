// ─── packages_universe ────────────────────────────────────────────────────────

export interface IDbPackageUniverse {
  id: string
  purl: string | null
  ecosystem: string
  namespace: string | null
  name: string
  rankInEcosystem: number | null
  isCritical: boolean
  dependentPackagesCount: number | null
  dependentReposCount: number | null
  downloads30d: bigint | null
}

// ─── packages ─────────────────────────────────────────────────────────────────

export type IDbPackageUpsert = {
  purl: string
  ecosystem: string
  namespace: string | null
  name: string
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  licenses: string[] | null
  licensesRaw: string | null
  latestVersion: string | null
  versionsCount?: number | null
  latestReleaseAt?: Date | null
  ingestionSource: string
  dependentPackagesCount?: number | null
  dependentReposCount?: number | null
  registryUrl?: string | null
  repositoryUrl?: string | null
}

// ─── sonatype popularity ──────────────────────────────────────────────────────

/**
 * Sonatype popularity signal for a single Maven component (one row per
 * groupId:artifactId). Only the sonatype_* fields plus the identity columns are
 * carried — deps.dev / Maven enrichment backfills everything else later.
 */
export type IDbSonatypePopularityUpsert = {
  purl: string
  ecosystem: string
  namespace: string // Maven groupId
  name: string // Maven artifactId
  sonatypePopularityScore: number
  sonatypeRank: number
  sonatypeTier: string
  sonatypeSnapshotAt: Date
}

// ─── maintainers ──────────────────────────────────────────────────────────────

export type IDbMaintainerUpsert = {
  ecosystem: string
  username: string
  displayName: string | null
  url: string | null
  email: string | null
  githubLogin?: string | null
}

// ─── package_maintainers ──────────────────────────────────────────────────────

export type IDbPackageMaintainerUpsert = {
  packageId: number
  maintainerId: number
  role: 'author' | 'maintainer' | 'contributor' | null
  ingestionSource?: string | null
}

// ─── versions ─────────────────────────────────────────────────────────────────

export type IDbVersionUpsert = {
  packageId: number
  ecosystem: string
  namespace: string | null
  name: string
  number: string
  isLatest: boolean
  isPrerelease: boolean
  license: string | null
}

// ─── repos ────────────────────────────────────────────────────────────────────

export type IDbRepoUpsert = {
  url: string
  host: string | null
  owner: string | null
  name: string | null
}

// ─── package_repos ────────────────────────────────────────────────────────────

export type IDbPackageRepoUpsert = {
  packageId: number
  repoId: number
  source: 'declared' | 'deps_dev' | 'heuristic' | 'manual'
  confidence: number
}
