import { ExtractorResult, ProvenanceEntry, RawContact, RepoPolicies } from '../../types'
import { extractEmails, fetchJson, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'pypi'

/* eslint-disable @typescript-eslint/no-explicit-any */

export function mapPypi(doc: unknown, sourceUrl: string, fetchedAt: string): ExtractorResult {
  const contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  if (!doc || typeof doc !== 'object') return { contacts, policies }
  const info = (doc as any).info
  if (!info || typeof info !== 'object') return { contacts, policies }

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const seen = new Set<string>()
  const addEmail = (email: string): void => {
    const key = email.toLowerCase()
    if (seen.has(key)) return
    seen.add(key)
    contacts.push({
      channel: 'email',
      value: email,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }

  // author_email / maintainer_email are RFC-5322 lists: "Name <a@b>, Name2 <c@d>"
  for (const field of ['author_email', 'maintainer_email']) {
    if (typeof info[field] === 'string') for (const e of extractEmails(info[field])) addEmail(e)
  }

  const projectUrls = info.project_urls
  if (projectUrls && typeof projectUrls === 'object') {
    for (const [key, value] of Object.entries(projectUrls)) {
      if (/security/i.test(key) && typeof value === 'string' && /^https?:\/\//i.test(value)) {
        if (!policies.securityPolicyUrl) policies.securityPolicyUrl = value
        contacts.push({
          channel: 'url',
          value,
          role: 'security-team',
          tier: 'B',
          provenance: prov(),
        })
      }
    }
  }

  return { contacts, policies }
}

export async function fetchPypi(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
): Promise<ExtractorResult> {
  const url = `https://pypi.org/pypi/${encodeURIComponent(parsed.name)}/json`
  const { json } = await fetchJson(url, timeoutMs, registryHeaders(userAgent))
  if (!json) return { contacts: [], policies: {} }
  return mapPypi(json, url, new Date().toISOString())
}
