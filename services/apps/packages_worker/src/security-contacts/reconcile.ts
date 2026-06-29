import { scoreContact } from './score'
import {
  ContactChannel,
  ContactRole,
  ProvenanceEntry,
  RawContact,
  ScoredContact,
  SourceTier,
} from './types'

const MAX_CONTACTS = 5

const ROLE_PRIORITY: Record<ContactRole, number> = {
  'security-team': 5,
  maintainer: 4,
  admin: 3,
  committer: 2,
  'org-owner': 1,
}

const TIER_RANK: Record<SourceTier, number> = { A: 4, B: 3, C: 2, D: 1 }

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

// Collapse a resolved handle into its email: extractors set the email contact's
// `name` to the originating handle, so a github-handle whose value matches becomes
// redundant once the email is known. The email (higher-quality) channel is kept.
function identityLinkMerge(contacts: RawContact[]): RawContact[] {
  const emailByHandle = new Map<string, RawContact>()
  for (const c of contacts) {
    if (c.channel === 'email' && c.name) emailByHandle.set(c.name.toLowerCase(), c)
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
  const merged = identityLinkMerge(exactMatchMerge(contacts))

  const scored: ScoredContact[] = merged.map((c) => {
    const contact = { ...c, provenance: dedupeProvenance(c.provenance) }
    return { ...contact, ...scoreContact(contact, now) }
  })

  scored.sort(
    (a, b) =>
      b.score - a.score ||
      ROLE_PRIORITY[b.role] - ROLE_PRIORITY[a.role] ||
      TIER_RANK[b.tier] - TIER_RANK[a.tier] ||
      a.value.localeCompare(b.value),
  )

  return scored.slice(0, MAX_CONTACTS)
}
