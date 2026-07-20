import { parseGitHubNoreplyEmail } from '@crowd/common'
import {
  findMembersByGithubHandles,
  findResolvableEmailsForMembers,
} from '@crowd/data-access-layer/src/members/identities'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { ProvenanceEntry, RawContact } from './types'

function latestTimestamp(provenance: ProvenanceEntry[]): string {
  const times = provenance.map((p) => p.declaredAt ?? p.fetchedAt)
  return times.length === 0
    ? new Date().toISOString()
    : times.reduce((a, b) => (new Date(b).getTime() > new Date(a).getTime() ? b : a))
}

// A GitHub noreply email already encodes its owner's handle — derive it so the handle can be
// resolved to a real address the same way any other github-handle contact is. The noreply email
// itself is kept as the provenance source, so the resolved contact's origin stays traceable.
export function deriveGithubHandlesFromNoreplyEmails(contacts: RawContact[]): RawContact[] {
  const derived: RawContact[] = []
  for (const c of contacts) {
    if (c.channel !== 'email') continue
    const handle = parseGitHubNoreplyEmail(c.value)
    if (!handle) continue
    derived.push({
      channel: 'github-handle',
      value: handle,
      role: c.role,
      tier: c.tier,
      provenance: [
        { source: c.value, sourceTier: c.tier, fetchedAt: latestTimestamp(c.provenance) },
      ],
    })
  }
  return derived
}

export async function resolveCdpEmails(
  cdpQx: QueryExecutor,
  handleContacts: RawContact[],
): Promise<RawContact[]> {
  if (handleContacts.length === 0) return []

  const handles = [...new Set(handleContacts.map((c) => c.value.toLowerCase()))]
  const members = await findMembersByGithubHandles(cdpQx, handles)
  if (members.length === 0) return []

  const memberIdsByHandle = new Map<string, string[]>()
  for (const m of members) {
    const key = m.githubHandle.toLowerCase()
    memberIdsByHandle.set(key, [...(memberIdsByHandle.get(key) ?? []), m.memberId])
  }

  const emails = await findResolvableEmailsForMembers(cdpQx, [
    ...new Set(members.map((m) => m.memberId)),
  ])
  const emailsByMember = new Map<string, { verified: string[]; unverified: string[] }>()
  for (const e of emails) {
    const bucket = emailsByMember.get(e.memberId) ?? { verified: [], unverified: [] }
    ;(e.verified ? bucket.verified : bucket.unverified).push(e.email)
    emailsByMember.set(e.memberId, bucket)
  }

  return handleContacts.flatMap((contact) => {
    const fetchedAt = latestTimestamp(contact.provenance)
    const memberIds = memberIdsByHandle.get(contact.value.toLowerCase()) ?? []
    return memberIds.flatMap((memberId) => {
      const bucket = emailsByMember.get(memberId)
      if (!bucket) return []
      const useVerified = bucket.verified.length > 0
      const source = useVerified ? 'cdp-verified' : 'cdp-unverified'
      return (useVerified ? bucket.verified : bucket.unverified).map((email) => ({
        channel: 'email' as const,
        value: email,
        role: contact.role,
        handle: contact.value,
        tier: contact.tier,
        provenance: [...contact.provenance, { source, sourceTier: contact.tier, fetchedAt }],
      }))
    })
  })
}
