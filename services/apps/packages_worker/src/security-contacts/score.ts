import { securityContactConfidenceBand } from '@crowd/data-access-layer/src/osspckgs/api'

import { ConfidenceBand, ContactChannel, ProvenanceEntry, RawContact, SourceTier } from './types'

const WEIGHTS = { tier: 0.55, channel: 0.2, freshness: 0.15, corroboration: 0.1 }

const TIER_SCORE: Record<SourceTier, number> = { A: 1.0, B: 0.7, C: 0.4, D: 0.2 }

// github-handle contacts carry no resolved email; nudge them below an equivalent email
const HANDLE_ONLY_PENALTY = 0.05

const FRESH_DAYS = 90
const STALE_DAYS = 730
const DAY_MS = 24 * 60 * 60 * 1000

const SECURITY_LOCALPARTS = new Set([
  'security',
  'secure',
  'psirt',
  'sirt',
  'cert',
  'cve',
  'abuse',
  'vuln',
  'vulnerability',
  'vulnerabilities',
  'disclosure',
])

const GENERIC_LOCALPARTS = new Set([
  'info',
  'team',
  'contact',
  'hello',
  'hi',
  'support',
  'admin',
  'help',
  'maintainers',
  'dev',
  'devs',
  'opensource',
  'open-source',
  'office',
  'mail',
])

function emailQuality(value: string): number {
  const localPart = value.split('@')[0]?.toLowerCase().trim() ?? ''
  if (SECURITY_LOCALPARTS.has(localPart) || localPart.startsWith('security')) return 1.0
  if (GENERIC_LOCALPARTS.has(localPart)) return 0.7
  return 0.6
}

function channelQuality(channel: ContactChannel, value: string): number {
  switch (channel) {
    case 'email':
      return emailQuality(value)
    case 'github-pvr':
      return 0.95
    case 'web-form':
    case 'url':
      return 0.5
    case 'github-handle':
      return 0.4
  }
}

function freshnessScore(provenance: ProvenanceEntry[], now: Date): number {
  const declaredTimes = provenance
    .map((p) => new Date(p.declaredAt ?? p.fetchedAt).getTime())
    .filter((t) => !Number.isNaN(t))
  if (declaredTimes.length === 0) return 0

  const ageDays = (now.getTime() - Math.max(...declaredTimes)) / DAY_MS
  if (ageDays <= FRESH_DAYS) return 1.0
  if (ageDays >= STALE_DAYS) return 0
  return 1 - (ageDays - FRESH_DAYS) / (STALE_DAYS - FRESH_DAYS)
}

// Independent = distinct extractors, not the same file re-fetched
function corroborationScore(provenance: ProvenanceEntry[]): number {
  const sources = new Set(provenance.map((p) => p.source))
  if (sources.size >= 3) return 1.0
  if (sources.size === 2) return 0.5
  return 0
}

export function confidenceBand(score: number): ConfidenceBand {
  return securityContactConfidenceBand(score)
}

export function scoreContact(
  contact: RawContact,
  now: Date = new Date(),
): { score: number; confidence: ConfidenceBand } {
  const raw =
    WEIGHTS.tier * TIER_SCORE[contact.tier] +
    WEIGHTS.channel * channelQuality(contact.channel, contact.value) +
    WEIGHTS.freshness * freshnessScore(contact.provenance, now) +
    WEIGHTS.corroboration * corroborationScore(contact.provenance) -
    (contact.channel === 'github-handle' ? HANDLE_ONLY_PENALTY : 0)

  const score = Math.round(Math.min(1, Math.max(0, raw)) * 1000) / 1000
  return { score, confidence: confidenceBand(score) }
}
