import { XMLParser } from 'fast-xml-parser'

import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { extractEmails, fetchJson, fetchText, registryHeaders } from '../http'

import { toHandleCandidates } from './handles'
import { ParsedPurl } from './purl'

const SOURCE = 'nuget-nuspec'
const SEARCH_SOURCE = 'nuget-search'
const BASE = 'https://api.nuget.org/v3-flatcontainer'
// Owner usernames are only exposed by the search service, not the flatcontainer API.
const SEARCH_BASE = 'https://azuresearch-usnc.nuget.org/query'
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

// NuGet.org accounts are independent of GitHub, so an owner username matching a GitHub login is
// only a guess — emitted as candidates that must pass repo-contributor corroboration before use.
export function mapNugetOwnerHandles(
  doc: unknown,
  packageId: string,
  sourceUrl: string,
  fetchedAt: string,
): RawContact[] {
  const data = (doc as any)?.data
  if (!Array.isArray(data)) return []
  // The exact package is not guaranteed to be the first result — scan for the id match,
  // same as fetchSearch in src/nuget/client.ts.
  const lowerId = packageId.toLowerCase()
  const entry = data.find((e) => typeof e?.id === 'string' && e.id.toLowerCase() === lowerId) as
    | Record<string, unknown>
    | undefined
  if (!entry || !Array.isArray(entry.owners)) return []
  return toHandleCandidates(entry.owners, SEARCH_SOURCE, sourceUrl, fetchedAt)
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
  const searchUrl = `${SEARCH_BASE}?q=packageid:${encodeURIComponent(id)}&prerelease=true&semVerLevel=2.0.0`
  const fetchedAt = new Date().toISOString()

  const [version, searchResult] = await Promise.all([
    latestStableVersion(id, timeoutMs, userAgent),
    fetchJson(searchUrl, timeoutMs, registryHeaders(userAgent)).catch(() => ({
      status: 0,
      json: null,
    })),
  ])

  const handleCandidates = mapNugetOwnerHandles(searchResult.json, id, searchUrl, fetchedAt)

  if (!version) return { contacts: [], policies: {}, handleCandidates }

  const url = `${BASE}/${id}/${version}/${id}.nuspec`
  const { text } = await fetchText(url, timeoutMs, registryHeaders(userAgent))
  if (!text) return { contacts: [], policies: {}, handleCandidates }
  return { contacts: mapNuspec(text, url, fetchedAt), policies: {}, handleCandidates }
}
