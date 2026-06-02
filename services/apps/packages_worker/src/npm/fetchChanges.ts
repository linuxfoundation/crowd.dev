import type { FetchError } from './types'

const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

const PAGE_LIMIT = 1000

export interface ChangesResult {
  names: string[]
  lastSeq: string
  hasMore: boolean
}

export async function fetchChangesSince(since: string): Promise<ChangesResult | FetchError> {
  const url = `https://replicate.npmjs.com/_changes?since=${encodeURIComponent(since)}&limit=${PAGE_LIMIT}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, {
      headers: { 'User-Agent': USER_AGENT, 'npm-replication-opt-in': 'true' },
      signal: abort.signal,
    })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

  let body: { results?: unknown[]; last_seq?: unknown; pending?: unknown }
  try {
    body = (await res.json()) as { results?: unknown[]; last_seq?: unknown; pending?: unknown }
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  if (!Array.isArray(body.results)) return { kind: 'MALFORMED', message: 'missing results' }

  const names = new Set<string>()
  for (const row of body.results as Array<{ id?: unknown }>) {
    if (typeof row.id === 'string' && !row.id.startsWith('_')) {
      names.add(row.id)
    }
  }

  const pending = typeof body.pending === 'number' ? body.pending : 0
  return { names: [...names], lastSeq: String(body.last_seq ?? since), hasMore: pending > 0 }
}

export async function fetchCurrentSeq(): Promise<string | FetchError> {
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch('https://replicate.npmjs.com/', {
      headers: { 'User-Agent': USER_AGENT, 'npm-replication-opt-in': 'true' },
      signal: abort.signal,
    })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}` }

  let body: { update_seq?: unknown }
  try {
    body = (await res.json()) as { update_seq?: unknown }
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  if (body.update_seq === undefined) return { kind: 'MALFORMED', message: 'missing update_seq' }
  return String(body.update_seq)
}
