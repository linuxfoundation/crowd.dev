import yaml from 'js-yaml'

import { getServiceChildLogger } from '@crowd/logging'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import {
  ContactChannel,
  ContactRole,
  Extractor,
  ExtractorResult,
  ProvenanceEntry,
  RawContact,
  RepoPolicies,
} from '../types'

import { RAW_BASE, fetchText, githubHandleFromUrl, isEmail } from './http'

const log = getServiceChildLogger('security-contacts:security-insights')

const SOURCE = 'security-insights'
const PATHS = [
  'SECURITY-INSIGHTS.yml',
  '.github/SECURITY-INSIGHTS.yml',
  '.gitlab/SECURITY-INSIGHTS.yml',
]

/* eslint-disable @typescript-eslint/no-explicit-any */

function arr(x: unknown): any[] {
  return Array.isArray(x) ? x : []
}

function classifyValue(raw: string): { channel: ContactChannel; value: string } | null {
  const v = raw.trim()
  if (!v) return null
  if (v.toLowerCase().startsWith('github:')) {
    const handle = v.slice('github:'.length).trim()
    return handle ? { channel: 'github-handle', value: handle } : null
  }
  if (isEmail(v)) return { channel: 'email', value: v }
  const handle = githubHandleFromUrl(v)
  if (handle) return { channel: 'github-handle', value: handle }
  if (/^https?:\/\//i.test(v)) return { channel: 'url', value: v }
  if (v.startsWith('@')) return { channel: 'github-handle', value: v.slice(1) }
  return { channel: 'github-handle', value: v }
}

// project-si-source may only point at trusted raw-content hosts (SSRF guard).
const REDIRECT_ALLOWED_HOSTS = new Set([
  'raw.githubusercontent.com',
  'gist.githubusercontent.com',
  'gitlab.com',
])

function isAllowedRedirect(url: string): boolean {
  try {
    const u = new URL(url)
    return u.protocol === 'https:' && REDIRECT_ALLOWED_HOSTS.has(u.hostname.toLowerCase())
  } catch {
    return false
  }
}

// MAINTAINERS.md / OWNERS.md links are documents, not contacts.
function isDocumentUrl(value: string): boolean {
  const v = value.toLowerCase()
  return v.includes('/blob/') || v.endsWith('.md')
}

type Mapped = { channel: ContactChannel; value: string; name?: string }

// security-contacts entry: typed object {type,value} or a bare string
function fromContactEntry(entry: unknown): Mapped | null {
  if (typeof entry === 'string') return classifyValue(entry)
  if (entry && typeof entry === 'object') {
    const o = entry as any
    const value = o.value ?? o.email ?? o.url
    if (typeof value !== 'string') return null
    const name = typeof o.name === 'string' ? o.name : undefined
    if (o.type === 'email') return { channel: 'email', value, name }
    if (o.type === 'url') return { channel: 'url', value, name }
    const c = classifyValue(value)
    return c ? { ...c, name } : null
  }
  return null
}

// people objects (core-team / administrators / champions / v2 contact): prefer email, else social handle
function fromPerson(p: unknown): Mapped | null {
  if (!p || typeof p !== 'object') return null
  const o = p as any
  const name = typeof o.name === 'string' ? o.name : undefined
  if (typeof o.email === 'string' && isEmail(o.email))
    return { channel: 'email', value: o.email, name }
  if (typeof o.social === 'string') {
    const c = classifyValue(o.social)
    if (c) return { ...c, name }
  }
  return null
}

export function mapSecurityInsights(
  doc: unknown,
  provPath: string,
  fetchedAt: string,
): ExtractorResult {
  const contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  if (doc == null || typeof doc !== 'object') return { contacts, policies }
  const root = doc as any

  const declaredAt: string | undefined =
    typeof root.header?.['last-updated'] === 'string'
      ? root.header['last-updated']
      : typeof root.header?.['last-reviewed'] === 'string'
        ? root.header['last-reviewed']
        : undefined

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'A', path: provPath, fetchedAt, declaredAt },
  ]

  const add = (m: Mapped | null, role: ContactRole): void => {
    if (m) contacts.push({ ...m, role, tier: 'A', provenance: prov() })
  }
  const setPolicy = (key: keyof RepoPolicies, value: unknown): void => {
    if (!policies[key] && typeof value === 'string' && value.trim()) {
      ;(policies as any)[key] = value.trim()
    }
  }

  // Scan flat (v1) and nested (v2 project/repository) layouts uniformly.
  const sections = [root, root.project, root.repository].filter((s) => s && typeof s === 'object')

  for (const section of sections) {
    for (const e of arr(section['security-contacts'])) add(fromContactEntry(e), 'security-team')

    const vr = section['vulnerability-reporting']
    if (vr && typeof vr === 'object') {
      if (typeof vr['email-contact'] === 'string' && isEmail(vr['email-contact'])) {
        add({ channel: 'email', value: vr['email-contact'] }, 'security-team')
      }
      add(fromPerson(vr['contact']), 'security-team')
      for (const e of arr(vr['security-contacts'])) add(fromContactEntry(e), 'security-team')
      setPolicy('securityPolicyUrl', vr['security-policy'])
      const programUrl = arr(vr['bug-bounty-programs']).find((p) => typeof p?.url === 'string')?.url
      setPolicy('bugBountyUrl', vr['bug-bounty-url'] ?? programUrl)
    }

    for (const p of arr(section['core-team'])) add(fromPerson(p), 'maintainer')
    for (const p of arr(section['administrators'])) add(fromPerson(p), 'maintainer')

    const security = section['security']
    if (security && typeof security === 'object') {
      for (const p of arr(security['champions'])) add(fromPerson(p), 'security-team')
    }

    const pl = section['project-lifecycle']
    if (pl && typeof pl === 'object') {
      for (const m of arr(pl['core-maintainers'])) {
        if (typeof m !== 'string') {
          add(fromPerson(m), 'maintainer')
          continue
        }
        const c = classifyValue(m)
        if (!c) continue
        // MAINTAINERS.md/OWNERS.md are non-standardized markdown — not safely parseable.
        // TODO(CM-1243): follow + extract maintainers from these documents in a later pass.
        if (c.channel === 'url' && isDocumentUrl(c.value)) {
          log.info(
            { provPath, document: c.value },
            'TODO(security-contacts): unparsed core-maintainers document',
          )
          continue
        }
        add(c, 'maintainer')
      }
    }

    const documentation = section['documentation']
    if (documentation && typeof documentation === 'object') {
      setPolicy('securityPolicyUrl', documentation['security-policy'])
    }
  }

  return { contacts, policies }
}

export function parseSecurityInsights(
  text: string,
  provPath: string,
  fetchedAt: string,
): ExtractorResult {
  let doc: unknown
  try {
    doc = yaml.load(text)
  } catch (err) {
    log.warn({ provPath, errMsg: (err as Error).message }, 'Failed to parse SECURITY-INSIGHTS.yml')
    return { contacts: [], policies: {} }
  }
  return mapSecurityInsights(doc, provPath, fetchedAt)
}

export const extractSecurityInsights: Extractor = async (target, deps) => {
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(target.url))
  } catch {
    return { contacts: [], policies: {} }
  }

  const fetchedAt = new Date().toISOString()

  for (const path of PATHS) {
    const { text } = await fetchText(
      `${RAW_BASE}/${owner}/${name}/HEAD/${path}`,
      deps.fetchTimeoutMs,
    )
    if (!text) continue

    let doc: unknown
    try {
      doc = yaml.load(text)
    } catch {
      continue
    }

    const redirect = (doc as any)?.header?.['project-si-source']
    // Only follow redirects to trusted raw-content hosts — a repo-controlled URL must not
    // be able to point the worker at internal/metadata endpoints (SSRF).
    if (typeof redirect === 'string' && isAllowedRedirect(redirect)) {
      const redirected = await fetchText(redirect, deps.fetchTimeoutMs)
      if (redirected.text) return parseSecurityInsights(redirected.text, redirect, fetchedAt)
    }

    return mapSecurityInsights(doc, path, fetchedAt)
  }

  return { contacts: [], policies: {} }
}
