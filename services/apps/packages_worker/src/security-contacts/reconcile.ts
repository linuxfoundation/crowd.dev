import { type EmailReachabilityReason, classifyEmailReachability } from '@crowd/common'

import { scoreContact } from './score'
import {
  ContactChannel,
  ContactRole,
  ProvenanceEntry,
  RawContact,
  ScoredContact,
  SourceTier,
} from './types'

const ROLE_PRIORITY: Record<ContactRole, number> = {
  'security-team': 5,
  maintainer: 4,
  admin: 3,
  committer: 2,
  'org-owner': 1,
}

const TIER_RANK: Record<SourceTier, number> = { A: 4, B: 3, C: 2, D: 1 }

// RFC 2606 reserved domains — always unedited template placeholders (e.g. security@example.com).
const PLACEHOLDER_EMAIL_DOMAINS = new Set([
  'example.com',
  'example.org',
  'example.net',
  'example.edu',
])

// Generic GitHub/Dependabot help pages that templated SECURITY.md files link to — never a
// project-specific contact, unlike an actual github.com/<owner>/<repo>/... URL.
const GENERIC_URL_HOSTS = new Set(['docs.github.com', 'dependabot.com', 'www.dependabot.com'])

const HAS_SCHEME_RE = /^[a-z][a-z0-9+.-]*:\/\//i

// Some upstream sources (e.g. SECURITY-INSIGHTS {type:"url"} entries) don't enforce a scheme —
// only add one to resolve the host, never mutate the stored contact value.
function urlHost(value: string): string | null {
  const candidate = HAS_SCHEME_RE.test(value) ? value : `https://${value}`
  try {
    return new URL(candidate).hostname.toLowerCase()
  } catch {
    return null
  }
}

function isJunkContact(c: RawContact): boolean {
  if (c.channel === 'email') {
    const domain = c.value.split('@')[1]?.toLowerCase().trim()
    return domain != null && PLACEHOLDER_EMAIL_DOMAINS.has(domain)
  }
  if (c.channel === 'url' || c.channel === 'web-form') {
    const host = urlHost(c.value)
    if (host == null) return true
    return GENERIC_URL_HOSTS.has(host) || host === 'localhost' || host.startsWith('127.')
  }
  return false
}

function classifyReachability(c: RawContact): {
  reachable: boolean
  reachabilityReason: EmailReachabilityReason | null
} {
  if (c.channel !== 'email') return { reachable: true, reachabilityReason: null }
  const r = classifyEmailReachability(c.value)
  return { reachable: r.reachable, reachabilityReason: r.reason }
}

function normalizeValue(channel: ContactChannel, value: string): string {
  const v = value.trim()
  return channel === 'email' || channel === 'github-handle' ? v.toLowerCase() : v
}

function higherRole(a: ContactRole, b: ContactRole): ContactRole {
  return ROLE_PRIORITY[a] >= ROLE_PRIORITY[b] ? a : b
}

function higherTier(a: SourceTier, b: SourceTier): SourceTier {
  return TIER_RANK[a] >= TIER_RANK[b] ? a : b
}

function dedupeProvenance(entries: ProvenanceEntry[]): ProvenanceEntry[] {
  const seen = new Set<string>()
  const out: ProvenanceEntry[] = []
  for (const e of entries) {
    const key = `${e.source}|${e.path ?? ''}|${e.fetchedAt}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push(e)
  }
  return out
}

function mergeInto(target: RawContact, src: RawContact): void {
  target.provenance.push(...src.provenance)
  target.role = higherRole(target.role, src.role)
  target.tier = higherTier(target.tier, src.tier)
  if (!target.name && src.name) target.name = src.name
  if (!target.handle && src.handle) target.handle = src.handle
}

function exactMatchMerge(contacts: RawContact[]): RawContact[] {
  const byKey = new Map<string, RawContact>()
  for (const c of contacts) {
    const key = `${c.channel}:${normalizeValue(c.channel, c.value)}`
    const existing = byKey.get(key)
    if (existing) {
      mergeInto(existing, c)
    } else {
      byKey.set(key, { ...c, provenance: [...c.provenance] })
    }
  }
  return [...byKey.values()]
}

// Collapse a bare github-handle into the email A3 resolved it from, matched via the explicit
// `handle` field (never the display name, to avoid merging unrelated people who share a name).
function identityLinkMerge(contacts: RawContact[]): RawContact[] {
  const emailByHandle = new Map<string, RawContact>()
  for (const c of contacts) {
    if (c.channel === 'email' && c.handle) emailByHandle.set(c.handle.toLowerCase(), c)
  }

  const out: RawContact[] = []
  for (const c of contacts) {
    if (c.channel === 'github-handle') {
      const email = emailByHandle.get(normalizeValue(c.channel, c.value))
      if (email) {
        mergeInto(email, c)
        continue
      }
    }
    out.push(c)
  }
  return out
}

export function reconcile(contacts: RawContact[], now: Date = new Date()): ScoredContact[] {
  const merged = identityLinkMerge(exactMatchMerge(contacts.filter((c) => !isJunkContact(c))))

  const scored: ScoredContact[] = merged.map((c) => {
    const contact = { ...c, provenance: dedupeProvenance(c.provenance) }
    return { ...contact, ...scoreContact(contact, now), ...classifyReachability(contact) }
  })

  scored.sort(
    (a, b) =>
      b.score - a.score ||
      ROLE_PRIORITY[b.role] - ROLE_PRIORITY[a.role] ||
      TIER_RANK[b.tier] - TIER_RANK[a.tier] ||
      a.value.localeCompare(b.value),
  )

  return scored
}
