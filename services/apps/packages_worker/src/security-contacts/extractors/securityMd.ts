import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import {
  ContactChannel,
  Extractor,
  ExtractorResult,
  ProvenanceEntry,
  RawContact,
  RepoPolicies,
} from '../types'

import { RAW_BASE, fetchText, githubHandleFromUrl } from './http'

const SOURCE = 'security.md'
const PATHS = ['SECURITY.md', '.github/SECURITY.md', 'docs/SECURITY.md']

const KEYWORD_RE =
  /\b(report|security|vulnerabilit(?:y|ies)|disclosure|contact|advisor(?:y|ies))\b/i
const NEGATIVE_SECTION_RE = /acknowledg|hall of fame|thanks|credits|honou?r|researchers/i
const EMAIL_RE = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g
const URL_RE = /https?:\/\/[^\s)<>\]"']+/g
const HEADING_RE = /^#{1,6}\s+(.*)$/

const PVR_RE = /private vulnerability reporting|\/security\/advisories\/new/i

function cleanUrl(url: string): string {
  return url.replace(/[.,;:]+$/, '')
}

export function parseSecurityMd(
  text: string,
  owner: string,
  name: string,
  provPath: string,
  fetchedAt: string,
): ExtractorResult {
  const policies: Partial<RepoPolicies> = {
    securityPolicyUrl: `https://github.com/${owner}/${name}/blob/HEAD/${provPath}`,
  }
  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'B', path: provPath, fetchedAt },
  ]

  const seen = new Set<string>()
  const contacts: RawContact[] = []
  const add = (channel: ContactChannel, value: string): boolean => {
    const key = `${channel}:${value.toLowerCase()}`
    if (seen.has(key)) return false
    seen.add(key)
    contacts.push({ channel, value, role: 'security-team', tier: 'B', provenance: prov() })
    return true
  }

  // Verbose policies list many references/people; bound how many B1 promotes.
  const MAX_PER_CHANNEL = 3
  let emailCount = 0
  let urlCount = 0

  const lines = text.split('\n')
  let heading = ''
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    const headingMatch = HEADING_RE.exec(line.trim())
    if (headingMatch) {
      heading = headingMatch[1]
      continue
    }

    // Skip researcher credits / acknowledgements sections — those are not contacts.
    if (NEGATIVE_SECTION_RE.test(heading)) continue
    // Paragraph-local proximity: a keyword must appear within ±2 lines of the candidate.
    const windowText = lines.slice(Math.max(0, i - 2), i + 3).join('\n')
    if (!KEYWORD_RE.test(windowText)) continue

    if (emailCount < MAX_PER_CHANNEL) {
      for (const email of line.match(EMAIL_RE) ?? []) {
        if (emailCount >= MAX_PER_CHANNEL) break
        if (add('email', email)) emailCount++
      }
    }
    if (urlCount < MAX_PER_CHANNEL) {
      for (const rawUrl of line.match(URL_RE) ?? []) {
        if (urlCount >= MAX_PER_CHANNEL) break
        const url = cleanUrl(rawUrl)
        // The canonical PVR advisory URL is emitted as a github-pvr contact below.
        if (/\/security\/advisories/i.test(url)) continue
        // Bare github.com/<handle> profile links are people listings, not reporting channels.
        if (githubHandleFromUrl(url)) continue
        if (add('url', url)) urlCount++
      }
    }
  }

  // PVR redirect language corroborates A2. Emitted unconditionally; processBatch vetoes it
  // when A2 authoritatively reports PVR disabled.
  if (PVR_RE.test(text)) {
    add('github-pvr', `https://github.com/${owner}/${name}/security/advisories/new`)
  }

  return { contacts, policies }
}

export const extractSecurityMd: Extractor = async (target, deps) => {
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
    if (text) return parseSecurityMd(text, owner, name, path, fetchedAt)
  }

  return { contacts: [], policies: {} }
}
