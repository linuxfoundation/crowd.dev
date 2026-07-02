import { XMLParser } from 'fast-xml-parser'

import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { fetchText, isEmail, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'maven-pom'
const BASE = 'https://repo1.maven.org/maven2'
const parser = new XMLParser({ ignoreAttributes: true })

/* eslint-disable @typescript-eslint/no-explicit-any */

function asArray<T>(x: T | T[] | undefined): T[] {
  if (x === undefined || x === null) return []
  return Array.isArray(x) ? x : [x]
}

export function mapMavenPom(xml: string, sourceUrl: string, fetchedAt: string): RawContact[] {
  let doc: any
  try {
    doc = parser.parse(xml)
  } catch {
    return []
  }
  const project = doc?.project
  if (!project) return []

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const contacts: RawContact[] = []
  const seen = new Set<string>()
  for (const dev of asArray(project.developers?.developer)) {
    const email = dev?.email
    if (typeof email !== 'string' || !isEmail(email)) continue
    const key = email.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    contacts.push({
      channel: 'email',
      value: email,
      name: typeof dev.name === 'string' ? dev.name : undefined,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }
  return contacts
}

async function resolveVersion(
  groupPath: string,
  artifact: string,
  timeoutMs: number,
  userAgent: string,
): Promise<string | null> {
  const url = `${BASE}/${groupPath}/${artifact}/maven-metadata.xml`
  const { text } = await fetchText(url, timeoutMs, registryHeaders(userAgent))
  if (!text) return null
  let doc: any
  try {
    doc = parser.parse(text)
  } catch {
    return null
  }
  const versioning = doc?.metadata?.versioning
  const release = versioning?.release ?? versioning?.latest
  if (typeof release === 'string') return release
  const versions = asArray(versioning?.versions?.version)
  return versions.length ? String(versions[versions.length - 1]) : null
}

export async function fetchMaven(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
): Promise<ExtractorResult> {
  if (!parsed.namespace) return { contacts: [], policies: {} }
  const groupPath = parsed.namespace.replace(/\./g, '/')
  const artifact = parsed.name

  const version = await resolveVersion(groupPath, artifact, timeoutMs, userAgent)
  if (!version) return { contacts: [], policies: {} }

  const url = `${BASE}/${groupPath}/${artifact}/${version}/${artifact}-${version}.pom`
  const { text } = await fetchText(url, timeoutMs, registryHeaders(userAgent))
  if (!text) return { contacts: [], policies: {} }
  return { contacts: mapMavenPom(text, url, new Date().toISOString()), policies: {} }
}
