import { resolveCurrentAffiliationsByMemberIds } from '@crowd/data-access-layer/src/affiliations'
import {
  IVerifiedIdentityMemberRow,
  findVerifiedMembersByIdentities,
} from '@crowd/data-access-layer/src/members/identities'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { MemberIdentityType } from '@crowd/types'

import { AKRITES_MEMBERS, AkritesMember, indexMembersByCdpOrganizationId } from './registry'
import { ContributorIdentity, ResolvedCdpAffiliation, contributorIdentityKey } from './types'

function splitIdentities(identities: ContributorIdentity[]) {
  const githubLogins = new Set<string>()
  const emails = new Set<string>()
  for (const identity of identities) {
    const value = identity.value.toLowerCase()
    if (identity.type === 'github-login') githubLogins.add(value)
    else emails.add(value)
  }
  return { githubLogins: [...githubLogins], emails: [...emails] }
}

function rowIdentityKey(row: IVerifiedIdentityMemberRow): string {
  const type = row.type === MemberIdentityType.USERNAME ? 'github-login' : 'email'
  return contributorIdentityKey({ type, value: row.value })
}

export async function resolveCdpAffiliations(
  cdpQx: QueryExecutor,
  identities: ContributorIdentity[],
  members: AkritesMember[] = AKRITES_MEMBERS,
): Promise<Map<string, ResolvedCdpAffiliation>> {
  const resolved = new Map<string, ResolvedCdpAffiliation>()
  if (identities.length === 0) return resolved

  const rows = await findVerifiedMembersByIdentities(cdpQx, splitIdentities(identities))
  if (rows.length === 0) return resolved

  const memberIds = [...new Set(rows.map((r) => r.memberId))]
  const affiliations = await resolveCurrentAffiliationsByMemberIds(cdpQx, memberIds)
  const membersByOrgId = indexMembersByCdpOrganizationId(members)

  for (const row of rows) {
    const affiliation = affiliations.get(row.memberId)
    const member = affiliation ? membersByOrgId.get(affiliation.organizationId) : undefined
    if (!member) continue
    resolved.set(rowIdentityKey(row), {
      memberId: row.memberId,
      displayName: row.displayName,
      akritesMember: member.name,
      cdpOrganizationId: affiliation.organizationId,
      confidence: 'cdp',
      confidenceScore: 1,
    })
  }
  return resolved
}
