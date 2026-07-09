export interface BatchResult {
  processed: number
  skipped: number
  error: number
  unchanged: number
}

export interface RubyGemsFetchError {
  kind: 'NOT_FOUND' | 'RATE_LIMIT'
  status?: number
  message: string
}

export type RubyGemsFetchResult<T> = T | RubyGemsFetchError

export function isRubyGemsFetchError<T>(r: RubyGemsFetchResult<T>): r is RubyGemsFetchError {
  return typeof r === 'object' && r !== null && 'kind' in (r as object)
}

export interface RubyGemsGemResponse {
  name: string
  version: string
  info?: string | null
  homepage_uri?: string | null
  source_code_uri?: string | null
  licenses?: string[] | null
  downloads?: number
}

export interface RubyGemsVersionItem {
  number: string
  created_at: string
  prerelease?: boolean
  licenses?: string[] | null
}

export interface RubyGemsOwner {
  handle?: string | null
  email?: string | null
}

export interface NormalizedRubyGemsPackage {
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  licenses: string[] | null
  licensesRaw: string | null
  latestVersion: string | null
  totalDownloads: number
}

export interface NormalizedRubyGemsVersion {
  number: string
  publishedAt: Date | null
  isPrerelease: boolean
  licenses: string[] | null
}

export interface NormalizedRubyGemsOwner {
  username: string
  email: string | null
}
