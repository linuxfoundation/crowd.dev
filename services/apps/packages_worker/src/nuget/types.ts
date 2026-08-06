import { CanonicalRepo } from '../utils/canonicalizeRepoUrl'

export interface NuGetConfig {
  batchSize: number
  concurrency: number
  groupDelayMs: number
  isCritical: boolean
  userAgent: string | undefined
}

export interface BatchResult {
  processed: number
  skipped: number
  error: number
  unchanged: number
}

export interface NuGetFetchError {
  kind: 'NOT_FOUND' | 'RATE_LIMIT'
  status?: number
  message: string
}

export type NuGetFetchResult<T> = T | NuGetFetchError

export function isNuGetFetchError<T>(r: NuGetFetchResult<T>): r is NuGetFetchError {
  return typeof r === 'object' && r !== null && 'kind' in (r as object)
}

export interface NuGetSearchItem {
  id: string
  version: string
  description?: string
  summary?: string
  authors?: string | string[]
  owners?: string[]
  projectUrl?: string
  totalDownloads?: number
  tags?: string[]
  versions?: Array<{ version: string; downloads: number }>
}

export interface NuGetCatalogEntry {
  id: string
  version: string
  description?: string
  authors?: string | string[]
  licenseExpression?: string
  licenseUrl?: string
  listed?: boolean
  published?: string
  projectUrl?: string
  deprecation?: { message?: string; alternatePackage?: unknown }
  repository?: { type?: string; url?: string }
}

export interface NuGetRegistrationLeaf {
  catalogEntry: NuGetCatalogEntry
}

export interface NuGetRegistrationPage {
  '@id': string
  count: number
  lower: string
  upper: string
  items?: NuGetRegistrationLeaf[]
}

export interface NuGetRegistrationIndex {
  count: number
  items: NuGetRegistrationPage[]
}

export interface NormalizedNuGetPackage {
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  repo: CanonicalRepo | null
  licenses: string[] | null
  licensesRaw: string | null
  keywords: string[] | null
  status: 'active' | 'deprecated' | 'unpublished'
  latestVersion: string | null
  versionsCount: number
  firstReleaseAt: Date | null
  latestReleaseAt: Date | null
  totalDownloads: number
  owners: string[]
  authors: string[]
  versions: NormalizedNuGetVersion[]
}

export interface NormalizedNuGetVersion {
  number: string
  publishedAt: Date | null
  isLatest: boolean
  isPrerelease: boolean
  isYanked: boolean
  licenses: string[] | null
  downloadCount: number | null
}
