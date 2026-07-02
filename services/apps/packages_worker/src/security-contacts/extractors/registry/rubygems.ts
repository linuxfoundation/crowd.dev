import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { extractEmails, fetchJson, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'rubygems'

/* eslint-disable @typescript-eslint/no-explicit-any */

// RubyGems hides author emails; we can only surface emails when an author string embeds one.
// bug_tracker_uri is an issue tracker, not a security contact, so it is intentionally skipped.
export function mapRubygems(doc: unknown, sourceUrl: string, fetchedAt: string): RawContact[] {
  if (!doc || typeof doc !== 'object') return []
  const authors = (doc as any).authors
  if (typeof authors !== 'string') return []

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const contacts: RawContact[] = []
  const seen = new Set<string>()
  for (const email of extractEmails(authors)) {
    const key = email.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    contacts.push({
      channel: 'email',
      value: email,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }
  return contacts
}

export async function fetchRubygems(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
): Promise<ExtractorResult> {
  const url = `https://rubygems.org/api/v1/gems/${encodeURIComponent(parsed.name)}.json`
  const { json } = await fetchJson(url, timeoutMs, registryHeaders(userAgent))
  if (!json) return { contacts: [], policies: {} }
  return { contacts: mapRubygems(json, url, new Date().toISOString()), policies: {} }
}
