import { Extractor, ExtractorResult, ProvenanceEntry, RawContact, RepoPolicies } from '../types'

import { fetchText, isEmail } from './http'

const SOURCE = 'security.txt'

// Platform hosts serve their own security.txt, not the project's — never attribute it to a repo.
const PLATFORM_HOSTS = new Set(['github.com', 'www.github.com', 'gitlab.com', 'bitbucket.org'])

const FIELD_RE = /^([A-Za-z-]+):\s*(.+?)\s*$/

// target.homepage is externally-sourced. Requiring https already blocks the classic SSRF target
// (cloud-metadata IMDS is http-only); we also reject obvious loopback/localhost.
function isBlockedHost(h: string): boolean {
  return h === 'localhost' || h === '::1' || h === '0.0.0.0' || h.startsWith('127.')
}

export function parseSecurityTxt(
  text: string,
  sourceUrl: string,
  fetchedAt: string,
): ExtractorResult {
  const contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}

  // Reject HTML served for a missing file (e.g. SPA 200s).
  if (/<!doctype|<html[\s>]/i.test(text)) return { contacts, policies }

  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'A', path: sourceUrl, fetchedAt },
  ]
  const add = (channel: RawContact['channel'], value: string): void => {
    contacts.push({ channel, value, role: 'security-team', tier: 'A', provenance: prov() })
  }

  let sawField = false
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) continue
    const m = FIELD_RE.exec(line)
    if (!m) continue
    const field = m[1].toLowerCase()
    const value = m[2].trim()

    if (field === 'contact') {
      sawField = true
      if (value.toLowerCase().startsWith('mailto:')) {
        const email = value.slice('mailto:'.length).trim()
        if (isEmail(email)) add('email', email)
      } else if (isEmail(value)) {
        add('email', value)
      } else if (/^https?:\/\//i.test(value)) {
        add('url', value)
      }
      // tel: and other schemes are not actionable channels — skip
    } else if (field === 'policy') {
      sawField = true
      if (!policies.securityPolicyUrl && /^https?:\/\//i.test(value))
        policies.securityPolicyUrl = value
    }
  }

  if (sawField) policies.securityTxtUrl = sourceUrl
  return { contacts, policies }
}

export const extractSecurityTxt: Extractor = async (target, deps) => {
  if (!target.homepage) return { contacts: [], policies: {} }

  let origin: string
  let host: string
  try {
    const u = new URL(target.homepage)
    if (u.protocol !== 'https:') return { contacts: [], policies: {} }
    origin = u.origin
    host = u.hostname.toLowerCase()
  } catch {
    return { contacts: [], policies: {} }
  }
  if (PLATFORM_HOSTS.has(host) || isBlockedHost(host)) return { contacts: [], policies: {} }

  const url = `${origin}/.well-known/security.txt`
  // The homepage is an arbitrary external host — frequently bot-protected or flaky (403/429/5xx/522,
  // DNS errors). A4 is a best-effort fallback, so a fetch failure must NOT fail the repo and discard
  // the other extractors' results (which would then never be written, since these hosts block us
  // persistently). Treat any error as "no security.txt".
  let text: string | null
  try {
    ;({ text } = await fetchText(url, deps.fetchTimeoutMs))
  } catch {
    return { contacts: [], policies: {} }
  }
  if (!text) return { contacts: [], policies: {} }

  return parseSecurityTxt(text, url, new Date().toISOString())
}
