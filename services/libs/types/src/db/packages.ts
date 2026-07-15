export interface PackageDbRow {
  id: string
  purl: string
  ecosystem: string
  namespace: string | null
  name: string
  registryUrl: string | null
  status: string | null
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
  licenses: string[] | null
  licensesRaw: string | null
  keywords: string[] | null
  distTagsLatest: string | null
  distTagsNext: string | null
  distTagsBeta: string | null
  versionsCount: number | null
  latestVersion: string | null
  firstReleaseAt: string | null
  latestReleaseAt: string | null
  dependentCount: number | null
  dependentReposCount: number | null
  hasCriticalVulnerability: boolean
  impact: number | null
  isCritical: boolean
  lastRankPassAt: string | null
  ingestionSource: string | null
  lastSyncedAt: string
  // bigint — pg returns these as strings by default (no OID 20 type-parser override
  // in @crowd/database), same reason `id` above is `string` not `number`.
  transitiveDependentCount: string | null
  createdAt: string
  updatedAt: string
  downloadsLast30d: string | null
  centralityScore: number | null
  rankInEcosystem: number | null
  lifecycleLabel: string | null
  healthScore: number | null
  healthLabel: string | null
  maintainerHealthScore: number | null
  securitySupplyChainScore: number | null
  developmentActivityScore: number | null
  signalCoverageHealth: Record<string, unknown> | null
  totalDownloads: string | null
  sonatypePopularityScore: number | null
  sonatypeRank: number | null
  sonatypeTier: string | null
  sonatypeSnapshotAt: string | null
  sonatypeUpdatedAt: string | null
}

export type PackageDbInsert = Pick<PackageDbRow, 'purl' | 'ecosystem' | 'name'> &
  Partial<
    Omit<
      PackageDbRow,
      'id' | 'purl' | 'ecosystem' | 'name' | 'createdAt' | 'updatedAt' | 'lastSyncedAt'
    >
  >
