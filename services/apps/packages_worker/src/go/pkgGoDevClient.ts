import { FetchError, GoStatusResult, isFetchError } from './types'

const BASE = process.env.PKGGODEV_BASE_URL ?? 'https://pkg.go.dev'
// 40 QPS per IP. Keep a single in-process gap under that ceiling.
const MIN_INTERVAL_MS = parseInt(process.env.PKGGODEV_MIN_INTERVAL_MS ?? '30', 10)
const MAX_429_RETRIES = 5
const MAX_PAGES = 20

interface VersionItem {
  version: string
  deprecated?: boolean
  retracted?: boolean
  latestVersion?: string
}
interface VersionsPage {
  items?: VersionItem[]
  total?: number
  nextPageToken?: string
}

let lastRequestAt = 0

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

async function throttle(): Promise<void> {
  const wait = lastRequestAt + MIN_INTERVAL_MS - Date.now()
  if (wait > 0) await sleep(wait)
  lastRequestAt = Date.now()
}

async function getPage(url: string, timeoutMs: number): Promise<VersionsPage | FetchError> {
  for (let attempt = 0; attempt <= MAX_429_RETRIES; attempt++) {
    await throttle()
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      clearTimeout(timer)
      return { kind: 'TRANSIENT', message: `network error: ${(e as Error).message}` }
    }
    clearTimeout(timer)

    if (res.status === 429) {
      const reset = parseInt(res.headers.get('x-ratelimit-reset') ?? '0', 10)
      const waitMs = reset ? Math.max(1000, reset * 1000 - Date.now() + 500) : 2000
      await sleep(waitMs)
      continue
    }
    // Any other 4xx is permanent (e.g. 400 for submodule/non-module-root paths) — skip, don't retry.
    if (res.status >= 400 && res.status < 500) {
      return { kind: 'NOT_FOUND', statusCode: res.status, message: `${res.status}` }
    }
    if (res.status !== 200) {
      return {
        kind: 'TRANSIENT',
        statusCode: res.status,
        message: `unexpected status ${res.status}`,
      }
    }
    try {
      return (await res.json()) as VersionsPage
    } catch {
      return { kind: 'MALFORMED', message: 'invalid json' }
    }
  }
  return { kind: 'RATE_LIMIT', statusCode: 429, message: '429 after retries' }
}

// Module status from /v1beta/versions: 'deprecated' if the latest version is
// deprecated/retracted, else 'active'. Pages are newest-first and every item carries
// latestVersion, so the match is normally on page 1; paginate (token query param,
// request otherwise verbatim) only if it isn't.
export async function fetchStatus(
  module: string,
  timeoutMs: number,
  onHeartbeat?: () => void,
): Promise<GoStatusResult | FetchError> {
  let token: string | undefined
  let versionsCount: number | null = null
  for (let page = 0; page < MAX_PAGES; page++) {
    const url = `${BASE}/v1beta/versions/${module}${token ? `?token=${encodeURIComponent(token)}` : ''}`
    const result = await getPage(url, timeoutMs)
    onHeartbeat?.()
    if (isFetchError(result)) return result

    if (versionsCount === null && typeof result.total === 'number') versionsCount = result.total
    const items = result.items ?? []
    const latestVersion = items.find((i) => i.latestVersion)?.latestVersion
    const match = latestVersion ? items.find((i) => i.version === latestVersion) : undefined
    if (match) {
      return {
        status: match.deprecated || match.retracted ? 'deprecated' : 'active',
        versionsCount,
      }
    }

    if (!result.nextPageToken) break
    token = result.nextPageToken
  }
  return { kind: 'NOT_FOUND', message: 'latest version entry not found' }
}
