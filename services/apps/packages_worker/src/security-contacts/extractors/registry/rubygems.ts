import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { extractEmails, fetchJson, isEmail, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'rubygems'

/* eslint-disable @typescript-eslint/no-explicit-any */

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

export function mapRubygemsOwners(
  owners: unknown,
  sourceUrl: string,
  fetchedAt: string,
): RawContact[] {
  if (!Array.isArray(owners)) return []

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const contacts: RawContact[] = []
  const seen = new Set<string>()
  for (const owner of owners) {
    const o = (owner ?? {}) as Record<string, unknown>
    const email = o.email
    if (typeof email !== 'string' || !isEmail(email)) continue
    const key = email.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    // RubyGems handles are not GitHub logins — stored as `name`, not `handle`, so
    // identityLinkMerge (reconcile.ts) never cross-links them to a github-handle contact.
    const name = typeof o.handle === 'string' ? o.handle : undefined
    contacts.push({
      channel: 'email',
      value: email,
      name,
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
  const gemUrl = `https://rubygems.org/api/v1/gems/${encodeURIComponent(parsed.name)}.json`
  const ownersUrl = `https://rubygems.org/api/v1/gems/${encodeURIComponent(parsed.name)}/owners.json`
  const fetchedAt = new Date().toISOString()

  const [gemResult, ownersResult] = await Promise.all([
    fetchJson(gemUrl, timeoutMs, registryHeaders(userAgent)),
    fetchJson(ownersUrl, timeoutMs, registryHeaders(userAgent)).catch(() => ({
      status: 0,
      json: null,
    })),
  ])

  if (!gemResult.json) return { contacts: [], policies: {} }

  const contacts = [
    ...mapRubygems(gemResult.json, gemUrl, fetchedAt),
    ...mapRubygemsOwners(ownersResult.json, ownersUrl, fetchedAt),
  ]

  return { contacts, policies: {} }
}
