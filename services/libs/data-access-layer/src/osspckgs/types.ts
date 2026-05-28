// ─── packages_universe ────────────────────────────────────────────────────────

export interface IDbPackageUniverse {
  id: number
  purl: string | null
  ecosystem: string
  namespace: string | null
  name: string
  rankInEcosystem: number | null
  isCritical: boolean
  criticalityScore: number | null
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
  ingestionSource: string
  criticalityScore?: number | null
  dependentPackagesCount?: number | null
  dependentReposCount?: number | null
  downloadsLastMonth?: bigint | null
  registryUrl?: string | null
  repositoryUrl?: string | null
}

// ─── maintainers ──────────────────────────────────────────────────────────────

export type IDbMaintainerUpsert = {
  ecosystem: string
  username: string
  displayName: string | null
  url: string | null
  emailHash: string | null
}

// ─── package_maintainers ──────────────────────────────────────────────────────

export type IDbPackageMaintainerUpsert = {
  packageId: number
  maintainerId: number
  role: 'author' | 'maintainer' | null
}
