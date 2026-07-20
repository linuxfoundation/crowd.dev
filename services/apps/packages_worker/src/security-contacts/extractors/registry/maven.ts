import { XMLParser } from 'fast-xml-parser'

import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'
import { fetchText, isEmail, registryHeaders } from '../http'

import { ParsedPurl } from './purl'

const SOURCE = 'maven-pom'
const BASE = 'https://repo1.maven.org/maven2'
// parseTagValue: false — the default coerces version strings to numbers ("3.0" -> 3,
// "1.10" -> 1.1), which builds wrong POM URLs for any artifact with a trailing-zero version.
const parser = new XMLParser({ ignoreAttributes: true, parseTagValue: false })

// Developer emails often live only in a shared parent POM (jackson-bom, eclipse-ee4j:project,
// commons-parent, …), so when a leaf yields none the parent chain is walked. Generic convenience
// parents (sonatype oss-parent, spring-boot-starter-parent) would donate maintainers unrelated
// to the project, so each level must prove relatedness before its contacts are accepted: shared
// groupId prefix (Central groups are verified publisher namespaces) or scm/url pointing at the
// target repo's owner. An unrelated level stops the walk — anything above it is even less related.
const MAX_PARENT_DEPTH = 5
const PARENT_CACHE_MAX = 5000

/* eslint-disable @typescript-eslint/no-explicit-any */

function asArray<T>(x: T | T[] | undefined): T[] {
  if (x === undefined || x === null) return []
  return Array.isArray(x) ? x : [x]
}

function parseProject(xml: string): any | null {
  let doc: any
  try {
    doc = parser.parse(xml)
  } catch {
    return null
  }
  return doc?.project ?? null
}

function mapProjectDevelopers(project: any, sourceUrl: string, fetchedAt: string): RawContact[] {
  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: sourceUrl, fetchedAt },
  ]
  const contacts: RawContact[] = []
  const seen = new Set<string>()
  for (const dev of asArray(project.developers?.developer)) {
    const email = (dev as any)?.email
    if (typeof email !== 'string' || !isEmail(email)) continue
    const key = email.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    contacts.push({
      channel: 'email',
      value: email,
      name: typeof (dev as any).name === 'string' ? (dev as any).name : undefined,
      role: 'maintainer',
      tier: 'B',
      provenance: prov(),
    })
  }
  return contacts
}

export function mapMavenPom(xml: string, sourceUrl: string, fetchedAt: string): RawContact[] {
  const project = parseProject(xml)
  if (!project) return []
  return mapProjectDevelopers(project, sourceUrl, fetchedAt)
}

export function pickVersion(metadataXml: string): string | null {
  let doc: any
  try {
    doc = parser.parse(metadataXml)
  } catch {
    return null
  }
  const versioning = doc?.metadata?.versioning
  const release = versioning?.release ?? versioning?.latest
  if (typeof release === 'string' && release.length > 0) return release
  const versions = asArray(versioning?.versions?.version)
  return versions.length ? String(versions[versions.length - 1]) : null
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
  return pickVersion(text)
}

interface ParentRef {
  group: string
  artifact: string
  version: string
}

function parentRef(project: any): ParentRef | null {
  const p = project?.parent
  if (
    typeof p?.groupId !== 'string' ||
    typeof p?.artifactId !== 'string' ||
    typeof p?.version !== 'string'
  ) {
    return null
  }
  // unflattened CI-friendly versions (${revision}) cannot be resolved to a registry path
  if (p.version.includes('${')) return null
  return { group: p.groupId, artifact: p.artifactId, version: p.version }
}

function groupRelated(leafGroup: string, parentGroup: string): boolean {
  return (
    parentGroup === leafGroup ||
    leafGroup.startsWith(`${parentGroup}.`) ||
    parentGroup.startsWith(`${leafGroup}.`)
  )
}

// Owner segment of a forge URL; Apache's svn/gitbox/git-wip hosts all map to the "apache" owner
// so github.com/apache scm entries match repos hosted on Apache infrastructure.
function repoOwnerOf(url: string): string | null {
  const m = /(?:github\.com|gitlab\.com|bitbucket\.org)[:/]+([^/:]+)[/:]/i.exec(url)
  if (m) return m[1].toLowerCase()
  if (/\bapache\.org\b/i.test(url)) return 'apache'
  return null
}

function scmOwners(project: any): string[] {
  const urls = [
    project?.scm?.url,
    project?.scm?.connection,
    project?.scm?.developerConnection,
    project?.url,
  ]
  const owners = new Set<string>()
  for (const u of urls) {
    if (typeof u !== 'string') continue
    const owner = repoOwnerOf(u)
    if (owner) owners.add(owner)
  }
  return [...owners]
}

interface ParentPom {
  group: string
  contacts: RawContact[]
  owners: string[]
  parent: ParentRef | null
}

// The same few parents sit above thousands of artifacts, so results are memoized per gav for
// the worker's lifetime. Failed fetches are evicted so a transient error is not pinned.
const parentPomCache = new Map<string, Promise<ParentPom | null>>()

async function fetchParentPom(
  ref: ParentRef,
  timeoutMs: number,
  userAgent: string,
): Promise<ParentPom | null> {
  const groupPath = ref.group.replace(/\./g, '/')
  const url = `${BASE}/${groupPath}/${ref.artifact}/${ref.version}/${ref.artifact}-${ref.version}.pom`
  const { text } = await fetchText(url, timeoutMs, registryHeaders(userAgent))
  if (!text) return null
  const project = parseProject(text)
  if (!project) return null
  return {
    group: typeof project.groupId === 'string' ? project.groupId : ref.group,
    contacts: mapProjectDevelopers(project, url, new Date().toISOString()),
    owners: scmOwners(project),
    parent: parentRef(project),
  }
}

function getParentPom(
  ref: ParentRef,
  timeoutMs: number,
  userAgent: string,
): Promise<ParentPom | null> {
  const key = `${ref.group}:${ref.artifact}:${ref.version}`
  const cached = parentPomCache.get(key)
  if (cached) return cached
  const pending = fetchParentPom(ref, timeoutMs, userAgent).catch(() => {
    parentPomCache.delete(key)
    return null
  })
  if (parentPomCache.size >= PARENT_CACHE_MAX) parentPomCache.clear()
  parentPomCache.set(key, pending)
  return pending
}

async function traverseParents(
  leafProject: any,
  leafGroup: string,
  repoUrl: string | undefined,
  timeoutMs: number,
  userAgent: string,
): Promise<RawContact[]> {
  const repoOwner = repoUrl ? repoOwnerOf(repoUrl) : null
  const seen = new Set<string>()
  let ref = parentRef(leafProject)
  for (let depth = 0; ref !== null && depth < MAX_PARENT_DEPTH; depth++) {
    const key = `${ref.group}:${ref.artifact}:${ref.version}`
    if (seen.has(key)) break
    seen.add(key)
    const pom = await getParentPom(ref, timeoutMs, userAgent)
    if (!pom) break
    const related =
      groupRelated(leafGroup, pom.group) || (repoOwner !== null && pom.owners.includes(repoOwner))
    if (!related) break
    if (pom.contacts.length > 0) return pom.contacts
    ref = pom.parent
  }
  return []
}

export async function fetchMaven(
  parsed: ParsedPurl,
  timeoutMs: number,
  userAgent: string,
  repoUrl?: string,
): Promise<ExtractorResult> {
  if (!parsed.namespace) return { contacts: [], policies: {} }
  const groupPath = parsed.namespace.replace(/\./g, '/')
  const artifact = parsed.name

  const version = await resolveVersion(groupPath, artifact, timeoutMs, userAgent)
  if (!version) return { contacts: [], policies: {} }

  const url = `${BASE}/${groupPath}/${artifact}/${version}/${artifact}-${version}.pom`
  const { text } = await fetchText(url, timeoutMs, registryHeaders(userAgent))
  if (!text) return { contacts: [], policies: {} }
  const project = parseProject(text)
  if (!project) return { contacts: [], policies: {} }

  let contacts = mapProjectDevelopers(project, url, new Date().toISOString())
  if (contacts.length === 0) {
    contacts = await traverseParents(project, parsed.namespace, repoUrl, timeoutMs, userAgent)
  }
  return { contacts, policies: {} }
}
