import type { FetchError } from './types'

const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

export interface ChangesResult {
  names: string[]
  lastSeq: string
}

export async function fetchChangesSince(since: string): Promise<ChangesResult | FetchError> {
  const names = new Set<string>()
  let seq = since

  for (;;) {
    const url = `https://replicate.npmjs.com/_changes?since=${encodeURIComponent(seq)}&limit=1000`
    let res: Response
    try {
      res = await fetch(url, {
        headers: { 'User-Agent': USER_AGENT, 'npm-replication-opt-in': 'true' },
      })
    } catch (err) {
      return { kind: 'TRANSIENT', message: String(err) }
    }

    if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

    let body: { results?: unknown[]; last_seq?: unknown; pending?: unknown }
    try {
      body = await res.json()
    } catch {
      return { kind: 'MALFORMED', message: 'invalid JSON' }
    }

    if (!Array.isArray(body.results)) return { kind: 'MALFORMED', message: 'missing results' }

    for (const row of body.results as Array<{ id?: unknown }>) {
      if (typeof row.id === 'string' && !row.id.startsWith('_')) {
        names.add(row.id)
      }
    }

    seq = String(body.last_seq ?? seq)
    if (!body.pending || body.pending === 0) break
  }

  return { names: [...names], lastSeq: seq }
}

export async function fetchCurrentSeq(): Promise<string | FetchError> {
  let res: Response
  try {
    res = await fetch('https://replicate.npmjs.com/', {
      headers: { 'User-Agent': USER_AGENT, 'npm-replication-opt-in': 'true' },
    })
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  }

  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}` }

  let body: { update_seq?: unknown }
  try {
    body = await res.json()
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  if (body.update_seq === undefined) return { kind: 'MALFORMED', message: 'missing update_seq' }
  return String(body.update_seq)
}
