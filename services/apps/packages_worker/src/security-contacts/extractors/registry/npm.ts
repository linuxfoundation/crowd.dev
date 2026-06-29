import { ProvenanceEntry, RawContact, RepoPackage } from '../../types'
import { fetchJson, isEmail } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'npm-registry'

/* eslint-disable @typescript-eslint/no-explicit-any */

function npmPackagePath(parsed: ParsedPurl): string {
  const full = parsed.namespace ? `${parsed.namespace}/${parsed.name}` : parsed.name
  // Scoped packages must percent-encode the slash for the registry path.
  return full.startsWith('@') ? full.replace('/', '%2F') : full
}

export function mapNpm(doc: unknown, sourceUrl: string, fetchedAt: string): RawContact[] {
  if (!doc || typeof doc !== 'object') return []
  const d = doc as any

  const declaredAt = typeof d.time?.modified === 'string' ? d.time.modified : undefined
  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt, declaredAt },
  ]

  const contacts: RawContact[] = []
  const seen = new Set<string>()
  const addEmail = (email: string, name?: string): void => {
    if (!isEmail(email)) return
    const key = email.toLowerCase()
    if (seen.has(key)) return
    seen.add(key)
    contacts.push({
      channel: 'email',
      value: email,
      name,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }

  if (typeof d.bugs?.email === 'string') addEmail(d.bugs.email)
  for (const m of Array.isArray(d.maintainers) ? d.maintainers : []) {
    if (typeof m?.email === 'string')
      addEmail(m.email, typeof m.name === 'string' ? m.name : undefined)
  }

  return contacts
}

export async function fetchNpm(
  parsed: ParsedPurl,
  _pkg: RepoPackage,
  timeoutMs: number,
): Promise<RawContact[]> {
  const url = `https://registry.npmjs.org/${npmPackagePath(parsed)}`
  const { json } = await fetchJson(url, timeoutMs)
  if (!json) return []
  return mapNpm(json, url, new Date().toISOString())
}
