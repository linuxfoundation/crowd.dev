export interface CargoConfig {
  dumpUrl: string
}

export interface LoadResult {
  crates: number
  versions: number
  dependencies: number
  versionDownloads: number
  owners: number
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
