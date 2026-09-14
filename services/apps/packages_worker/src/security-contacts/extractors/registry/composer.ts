import { ExtractorResult, ProvenanceEntry, RawContact, RepoPolicies } from '../../types'
import { fetchJson, isEmail, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'packagist'

/* eslint-disable @typescript-eslint/no-explicit-any */

export function mapComposer(
  doc: unknown,
  fullName: string,
  sourceUrl: string,
  fetchedAt: string,
): ExtractorResult {
  const contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  if (!doc || typeof doc !== 'object') return { contacts, policies }

  const versions = (doc as any).packages?.[fullName]
  const latest = Array.isArray(versions) ? versions[0] : undefined
  if (!latest || typeof latest !== 'object') return { contacts, policies }

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const seen = new Set<string>()
  const add = (
    channel: RawContact['channel'],
    value: string,
    role: RawContact['role'],
    name?: string,
  ): void => {
    const key = `${channel}:${value.toLowerCase()}`
    if (seen.has(key)) return
    seen.add(key)
    contacts.push({ channel, value, name, role, tier: 'B', provenance: prov() })
  }

  for (const a of Array.isArray(latest.authors) ? latest.authors : []) {
    if (typeof a?.email === 'string' && isEmail(a.email)) {
      add('email', a.email, 'maintainer', typeof a.name === 'string' ? a.name : undefined)
    }
  }

  const support = latest.support
  if (support && typeof support === 'object') {
    // support.security is Composer's dedicated security channel.
    if (typeof support.security === 'string' && /^https?:\/\//i.test(support.security)) {
      policies.securityPolicyUrl = support.security
      add('url', support.security, 'security-team')
    }
    if (typeof support.email === 'string' && isEmail(support.email)) {
      add('email', support.email, 'security-team')
    }
  }

  return { contacts, policies }
}

export async function fetchComposer(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
): Promise<ExtractorResult> {
  if (!parsed.namespace) return { contacts: [], policies: {} }
  const fullName = `${parsed.namespace}/${parsed.name}`
  const url = `https://repo.packagist.org/p2/${fullName}.json`
  const { json } = await fetchJson(url, timeoutMs, registryHeaders(userAgent))
  if (!json) return { contacts: [], policies: {} }
  return mapComposer(json, fullName, url, new Date().toISOString())
}
