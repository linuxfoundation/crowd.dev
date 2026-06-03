export interface DockerhubRepoResult {
  imageName: string // '<namespace>/<name>', lowercase
  pulls: number
  stars: number
  lastUpdated: string | null
}

export interface DiscoveryRepoRow {
  id: string
  url: string
  owner: string | null
  name: string | null
}

export interface RefreshImageRow {
  id: string
  repo_id: string | null
  image_name: string
}

export type FetchErrorKind = 'RATE_LIMIT' | 'TRANSIENT' | 'NOT_FOUND' | 'AUTH' | 'MALFORMED'

export class FetchError extends Error {
  constructor(
    public readonly kind: FetchErrorKind,
    message: string,
    public readonly resetAt?: number,
  ) {
    super(message)
    this.name = 'FetchError'
  }
}
