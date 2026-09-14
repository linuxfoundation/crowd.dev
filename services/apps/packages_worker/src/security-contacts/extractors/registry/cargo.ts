import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { fetchJson, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'crates.io'

// crates.io policy: max 1 request/second. Serialize calls process-wide.
// https://crates.io/policies#crawlers
let nextAllowedAt = 0
async function throttle(): Promise<void> {
  const now = Date.now()
  const waitMs = Math.max(0, nextAllowedAt - now)
  nextAllowedAt = Math.max(now, nextAllowedAt) + 1000
  if (waitMs > 0) await new Promise((r) => setTimeout(r, waitMs))
}

/* eslint-disable @typescript-eslint/no-explicit-any */

export function mapCargoOwners(doc: unknown, sourceUrl: string, fetchedAt: string): RawContact[] {
  if (!doc || typeof doc !== 'object') return []
  const users = (doc as any).users
  if (!Array.isArray(users)) return []

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const contacts: RawContact[] = []
  const seen = new Set<string>()
  for (const u of users) {
    const login = u?.login
    // crates.io logins like "github:org:team" are teams, not personal handles.
    if (typeof login !== 'string' || login.includes(':')) continue
    const key = login.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    contacts.push({
      channel: 'github-handle',
      value: login,
      name: typeof u.name === 'string' ? u.name : undefined,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }
  return contacts
}

export async function fetchCargo(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
): Promise<ExtractorResult> {
  // crates.io does not expose author emails; owners give GitHub handles.
  await throttle()
  const url = `https://crates.io/api/v1/crates/${encodeURIComponent(parsed.name)}/owners`
  const { json } = await fetchJson(url, timeoutMs, registryHeaders(userAgent))
  if (!json) return { contacts: [], policies: {} }
  return { contacts: mapCargoOwners(json, url, new Date().toISOString()), policies: {} }
}
