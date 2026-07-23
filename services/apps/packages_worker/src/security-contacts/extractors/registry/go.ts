import { ExtractorResult, ProvenanceEntry, RawContact } from '../../types'

const SOURCE = 'go.dev/security-policy'
const POLICY_URL = 'https://go.dev/doc/security/policy#reporting-a-security-bug'

export async function fetchGo(): Promise<ExtractorResult> {
  const fetchedAt = new Date().toISOString()
  const prov = (): ProvenanceEntry[] => [
    { source: SOURCE, sourceTier: 'D', path: POLICY_URL, fetchedAt },
  ]
  const contacts: RawContact[] = [
    {
      channel: 'email',
      value: 'security@golang.org',
      role: 'security-team',
      tier: 'D',
      provenance: prov(),
    },
    {
      channel: 'web-form',
      value: 'https://g.co/vulnz',
      role: 'security-team',
      tier: 'D',
      provenance: prov(),
    },
  ]
  return { contacts, policies: {} }
}
