export interface GoProxyLatest {
  version: string
  releaseAt: string | null
  repoUrl: string | null
}

export type GoStatus = 'active' | 'deprecated'

export interface GoStatusResult {
  status: GoStatus
  versionsCount: number | null
}

export interface FetchError {
  kind: 'NOT_FOUND' | 'RATE_LIMIT' | 'TRANSIENT' | 'MALFORMED'
  statusCode?: number
  message: string
}

export function isFetchError(v: unknown): v is FetchError {
  return typeof v === 'object' && v !== null && 'kind' in v && 'message' in v
}
