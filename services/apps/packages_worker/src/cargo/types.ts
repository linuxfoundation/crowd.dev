export interface CargoConfig {
  dumpUrl: string
  dumpDir: string
}

export interface LoadResult {
  crates: number
  versions: number
  dependencies: number
  versionDownloads: number
  owners: number
  // Crates whose purl matched an existing cargo package row — the actionable set
  // every enrich phase operates on.
  matched: number
  durationMs: number
}

export interface EnrichPackagesResult {
  updated: number
}

export interface EnrichVersionsResult {
  upserted: number
}

export interface EnrichReposResult {
  repos: number
  links: number
}

export interface EnrichMaintainersResult {
  maintainers: number
  links: number
}

export interface EnrichDownloadsDailyResult {
  inserted: number
}

export interface CargoSyncResult {
  load: LoadResult
  packages: EnrichPackagesResult
  versions: EnrichVersionsResult
  repos: EnrichReposResult
  maintainers: EnrichMaintainersResult
  downloadsDaily: EnrichDownloadsDailyResult
}
