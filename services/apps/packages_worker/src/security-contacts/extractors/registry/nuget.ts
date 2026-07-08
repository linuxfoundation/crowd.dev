import { XMLParser } from 'fast-xml-parser'

import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { extractEmails, fetchJson, fetchText, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'nuget-nuspec'
const BASE = 'https://api.nuget.org/v3-flatcontainer'
const parser = new XMLParser({ ignoreAttributes: true })

/* eslint-disable @typescript-eslint/no-explicit-any */

// NuGet authors/owners are display names; emails appear only when an author string embeds one.
export function mapNuspec(xml: string, sourceUrl: string, fetchedAt: string): RawContact[] {
  let doc: any
  try {
    doc = parser.parse(xml)
  } catch {
    return []
  }
  const meta = doc?.package?.metadata
  if (!meta) return []

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const contacts: RawContact[] = []
  const seen = new Set<string>()
  for (const field of ['authors', 'owners']) {
    if (typeof meta[field] !== 'string') continue
    for (const email of extractEmails(meta[field])) {
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
  }
  return contacts
}

async function latestStableVersion(
  id: string,
  timeoutMs: number,
  userAgent: string,
): Promise<string | null> {
  const { json } = await fetchJson(
    `${BASE}/${id}/index.json`,
    timeoutMs,
    registryHeaders(userAgent),
  )
  const versions = (json as { versions?: unknown } | null)?.versions
  if (!Array.isArray(versions) || versions.length === 0) return null
  const stable = [...versions].reverse().find((v) => typeof v === 'string' && !v.includes('-'))
  return (stable as string) ?? (versions[versions.length - 1] as string)
}

export async function fetchNuget(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
): Promise<ExtractorResult> {
  const id = parsed.name.toLowerCase()
  const version = await latestStableVersion(id, timeoutMs, userAgent)
  if (!version) return { contacts: [], policies: {} }

  const url = `${BASE}/${id}/${version}/${id}.nuspec`
  const { text } = await fetchText(url, timeoutMs, registryHeaders(userAgent))
  if (!text) return { contacts: [], policies: {} }
  return { contacts: mapNuspec(text, url, new Date().toISOString()), policies: {} }
}
