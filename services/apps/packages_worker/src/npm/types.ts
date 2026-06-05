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

export type FetchErrorKind = 'RATE_LIMIT' | 'TRANSIENT' | 'NOT_FOUND' | 'MALFORMED'

export interface FetchError {
  kind: FetchErrorKind
  message: string
  statusCode?: number
}

export function isFetchError(v: unknown): v is FetchError {
  return typeof v === 'object' && v !== null && 'kind' in v && 'message' in v
}
