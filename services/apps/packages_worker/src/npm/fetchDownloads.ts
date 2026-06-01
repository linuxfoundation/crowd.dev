import type { FetchError } from './types'

const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

function encodeNpmName(name: string): string {
  return name.startsWith('@') ? `@${encodeURIComponent(name.slice(1))}` : encodeURIComponent(name)
}

export interface PointRangeResult {
  count: number
  start: string
  end: string
}

export interface BulkPointRangeResult {
  counts: Map<string, number> // name → download count (absent = not found / no data)
  start: string
  end: string
}

// GET api.npmjs.org/downloads/point/<start>:<end>/<pkg1>,<pkg2>,...
// Supports up to 128 unscoped packages per request. Scoped packages are not supported.
// Packages with no data are returned as null in the response and are absent from counts.
export async function fetchBulkPointRange(
  names: string[],
  start: string,
  end: string,
): Promise<BulkPointRangeResult | FetchError> {
  if (names.length > 128)
    throw new Error(`fetchBulkPointRange: too many names (${names.length} > 128)`)
  const url = `https://api.npmjs.org/downloads/point/${start}:${end}/${names.join(',')}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, { headers: { 'User-Agent': USER_AGENT }, signal: abort.signal })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: 'NOT_FOUND', message: `bulk request not found`, statusCode: 404 }
  if (res.status === 429) {
    const headers: Record<string, string> = {}
    for (const [k, v] of res.headers.entries()) headers[k] = v
    const body = await res.text().catch(() => '')
    return {
      kind: 'RATE_LIMIT',
      message: `rate limited by npm downloads API — headers: ${JSON.stringify(headers)} body: ${body}`,
      statusCode: 429,
    }
  }
  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  if (typeof json !== 'object' || json === null)
    return { kind: 'MALFORMED', message: 'expected object response' }

  const raw = json as Record<string, { downloads?: unknown; start?: unknown; end?: unknown } | null>
  const counts = new Map<string, number>()
  let respStart = start
  let respEnd = end

  for (const [name, entry] of Object.entries(raw)) {
    if (entry === null || typeof entry.downloads !== 'number') continue
    counts.set(name, entry.downloads)
    if (typeof entry.start === 'string') respStart = entry.start
    if (typeof entry.end === 'string') respEnd = entry.end
  }

  return { counts, start: respStart, end: respEnd }
}

// GET api.npmjs.org/downloads/point/<start>:<end>/<name> — returns total downloads for the range.
export async function fetchPointRange(
  name: string,
  start: string,
  end: string,
): Promise<PointRangeResult | FetchError> {
  const url = `https://api.npmjs.org/downloads/point/${start}:${end}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, { headers: { 'User-Agent': USER_AGENT }, signal: abort.signal })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: 'NOT_FOUND', message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) {
    const headers: Record<string, string> = {}
    for (const [k, v] of res.headers.entries()) headers[k] = v
    const body = await res.text().catch(() => '')
    return {
      kind: 'RATE_LIMIT',
      message: `rate limited by npm downloads API — headers: ${JSON.stringify(headers)} body: ${body}`,
      statusCode: 429,
    }
  }
  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  const data = json as { downloads?: unknown; start?: unknown; end?: unknown }
  if (typeof data.downloads !== 'number')
    return { kind: 'MALFORMED', message: 'missing downloads field' }

  return {
    count: data.downloads,
    start: typeof data.start === 'string' ? data.start : start,
    end: typeof data.end === 'string' ? data.end : end,
  }
}

export interface DailyDownloadsResult {
  start: string
  end: string
  downloads: Array<{ day: string; downloads: number }>
}

// GET api.npmjs.org/downloads/range/<start>:<end>/<name> — max 18 months per call.
export async function fetchDailyRange(
  name: string,
  start: string,
  end: string,
): Promise<DailyDownloadsResult | FetchError> {
  const url = `https://api.npmjs.org/downloads/range/${start}:${end}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, { headers: { 'User-Agent': USER_AGENT }, signal: abort.signal })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: 'NOT_FOUND', message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) {
    const headers: Record<string, string> = {}
    for (const [k, v] of res.headers.entries()) headers[k] = v
    const body = await res.text().catch(() => '')
    return {
      kind: 'RATE_LIMIT',
      message: `rate limited by npm downloads API — headers: ${JSON.stringify(headers)} body: ${body}`,
      statusCode: 429,
    }
  }
  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  const data = json as { start?: unknown; end?: unknown; downloads?: unknown }
  if (!Array.isArray(data.downloads))
    return { kind: 'MALFORMED', message: 'missing downloads array' }

  return {
    start: typeof data.start === 'string' ? data.start : start,
    end: typeof data.end === 'string' ? data.end : end,
    downloads: (data.downloads as Array<{ day: unknown; downloads: unknown }>)
      .filter((d) => typeof d.day === 'string' && typeof d.downloads === 'number')
      .map((d) => ({ day: d.day as string, downloads: d.downloads as number })),
  }
}
