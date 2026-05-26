// ─── packages_universe ────────────────────────────────────────────────────────

export interface IDbPackageUniverse {
  id: number
  purl: string | null
  ecosystem: string
  namespace: string | null
  name: string
  rankInEcosystem: number | null
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
