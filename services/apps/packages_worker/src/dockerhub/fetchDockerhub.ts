import { DockerhubRepoResult, FetchError } from './types'

interface HubResponse {
  name?: string
  namespace?: string
  pull_count?: number
  star_count?: number
  last_updated?: string | null
}

// imageName must be '<namespace>/<name>' (lowercase). Trailing slash on the URL
// is required — Hub returns a 301 to the slashed form otherwise.
export async function fetchDockerhub(
  baseUrl: string,
  imageName: string,
): Promise<DockerhubRepoResult> {
  const url = `${baseUrl}/repositories/${imageName}/`

  let response: Response
  try {
    response = await fetch(url, {
      method: 'GET',
      headers: { Accept: 'application/json' },
    })
  } catch (err) {
    throw new FetchError('TRANSIENT', `Network error for ${imageName}: ${(err as Error).message}`)
  }

  const remaining = parseInt(response.headers.get('x-ratelimit-remaining') ?? '', 10)
  const resetSec = parseInt(response.headers.get('x-ratelimit-reset') ?? '0', 10)
  const resetMs = resetSec ? resetSec * 1000 + 5_000 : Date.now() + 65_000

  if (response.status === 429 || (Number.isFinite(remaining) && remaining <= 0)) {
    throw new FetchError('RATE_LIMIT', `Rate limited on ${imageName}`, resetMs)
  }

  if (response.status === 404) throw new FetchError('NOT_FOUND', `404 for ${imageName}`)
  if (response.status >= 500) {
    throw new FetchError('TRANSIENT', `${response.status} for ${imageName}`)
  }
  if (!response.ok) {
    // 400/401/403 etc — treat as a miss; Hub sometimes 400s on malformed slugs.
    throw new FetchError('NOT_FOUND', `${response.status} for ${imageName}`)
  }

  let json: HubResponse
  try {
    json = (await response.json()) as HubResponse
  } catch (err) {
    throw new FetchError('MALFORMED', `Non-JSON body for ${imageName}: ${(err as Error).message}`)
  }

  if (typeof json.pull_count !== 'number') {
    throw new FetchError('MALFORMED', `Missing pull_count for ${imageName}`)
  }

  return {
    imageName,
    pulls: json.pull_count,
    stars: json.star_count ?? 0,
    lastUpdated: json.last_updated ?? null,
  }
}
