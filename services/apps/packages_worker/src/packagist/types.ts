import type { PackageMaintainerInput } from '@crowd/data-access-layer/src/packages/maintainers'

export type FetchErrorKind = 'RATE_LIMIT' | 'TRANSIENT' | 'NOT_FOUND' | 'MALFORMED'

export interface FetchError {
  kind: FetchErrorKind
  message: string
  statusCode?: number
}

const FETCH_ERROR_KINDS: ReadonlySet<string> = new Set([
  'RATE_LIMIT',
  'TRANSIENT',
  'NOT_FOUND',
  'MALFORMED',
])

// Explicit kind check (unlike pypi's loose `'kind' in v`) because the p2 fetch has a
// non-error NOT_MODIFIED outcome that also carries a `kind`.
export function isFetchError(v: unknown): v is FetchError {
  return (
    typeof v === 'object' &&
    v !== null &&
    'kind' in v &&
    FETCH_ERROR_KINDS.has((v as { kind: string }).kind)
  )
}

// Subset of packagist.org/packages/{vendor}/{name}.json (dynamic stats endpoint) we consume.
export interface PackagistMaintainer {
  name?: string
  avatar_url?: string
}

export interface PackagistDownloads {
  total?: number
  monthly?: number
  daily?: number
}

export interface PackagistPackageInfo {
  name: string
  description?: string | null
  time?: string | null
  maintainers?: PackagistMaintainer[]
  type?: string | null
  repository?: string | null
  language?: string | null
  // Absent/false when maintained; true or a replacement package name when abandoned.
  abandoned?: boolean | string
  dependents?: number
  suggesters?: number
  favers?: number
  downloads?: PackagistDownloads
}

export interface PackagistStatsJson {
  package: PackagistPackageInfo
}

export interface NormalizedPackagistStats {
  // vendor/name, as Packagist reports it
  name: string
  description: string | null
  repositoryUrl: string | null
  status: 'active' | 'deprecated'
  dependents: number | null
  downloadsTotal: number | null
  downloadsMonthly: number | null
  maintainers: PackageMaintainerInput[]
}

// repo.packagist.org/p2/{vendor}/{name}.json — Composer-minified version objects.
// The first object is complete; each subsequent one carries only changed keys, with
// the literal string '__unset' marking a removed key.
export type PackagistMinifiedVersion = { version: string } & Record<string, unknown>

export interface PackagistExpandedVersion {
  version: string
  version_normalized?: string
  time?: string
  license?: string[]
  homepage?: string
  require?: Record<string, string>
  'require-dev'?: Record<string, string>
  [key: string]: unknown
}

export interface P2Metadata {
  minifiedVersions: PackagistMinifiedVersion[]
  // Last-Modified response header, replayed as If-Modified-Since on the next fetch.
  lastModified: string | null
}

export interface P2NotModified {
  kind: 'NOT_MODIFIED'
}

export function isP2NotModified(v: unknown): v is P2NotModified {
  return typeof v === 'object' && v !== null && (v as { kind?: string }).kind === 'NOT_MODIFIED'
}

export interface PackagistVersionRow {
  number: string
  publishedAt: string | null
  isLatest: boolean
  isPrerelease: boolean
  licenses: string[] | null
}

export interface PackagistDependency {
  name: string
  constraint: string
  kind: 'direct' | 'dev'
}
