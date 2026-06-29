import { Extractor, ExtractorResult, ProvenanceEntry, RawContact, RepoPolicies } from '../types'

import { fetchText, isEmail } from './http'

const SOURCE = 'security.txt'

// Platform hosts serve their own security.txt, not the project's — never attribute it to a repo.
const PLATFORM_HOSTS = new Set(['github.com', 'www.github.com', 'gitlab.com', 'bitbucket.org'])

const FIELD_RE = /^([A-Za-z-]+):\s*(.+?)\s*$/

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
    origin = u.origin
    host = u.hostname.toLowerCase()
  } catch {
    return { contacts: [], policies: {} }
  }
  if (PLATFORM_HOSTS.has(host)) return { contacts: [], policies: {} }

  const url = `${origin}/.well-known/security.txt`
  const { text } = await fetchText(url, deps.fetchTimeoutMs)
  if (!text) return { contacts: [], policies: {} }

  return parseSecurityTxt(text, url, new Date().toISOString())
}
