export type FundingEntry = string | { type?: string; url: string }

export interface PackumentVersion {
  version: string
  description?: string
  license?: string
  deprecated?: string
  funding?: FundingEntry | FundingEntry[]
}

export interface Packument {
  name: string
  description?: string
  homepage?: string
  keywords?: string[]
  license?: string | { type: string; url?: string }
  licenses?: Array<{ type: string; url?: string }>
  repository?: string | { type?: string; url: string; directory?: string }
  author?: string | { name: string; email?: string; url?: string }
  maintainers?: Array<{ name: string; email?: string }>
  'dist-tags': Record<string, string>
  versions: Record<string, PackumentVersion>
  time: Record<string, string>
  unpublished?: unknown
}

export enum FetchErrorKind {
  RATE_LIMIT = 'RATE_LIMIT',
  TRANSIENT = 'TRANSIENT',
  NOT_FOUND = 'NOT_FOUND',
  MALFORMED = 'MALFORMED',
}

export interface FetchError {
  kind: FetchErrorKind
  message: string
  statusCode?: number
  // Server-stated wait (Retry-After) on RATE_LIMIT, so retries can honor the real
  // penalty window instead of guessing with exponential backoff.
  retryAfterSec?: number
}

export function isFetchError(v: unknown): v is FetchError {
  return typeof v === 'object' && v !== null && 'kind' in v && 'message' in v
}
