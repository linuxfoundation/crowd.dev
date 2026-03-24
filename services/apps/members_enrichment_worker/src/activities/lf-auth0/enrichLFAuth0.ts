import { DEFAULT_TENANT_ID } from '@crowd/common'
import { PgPromiseQueryExecutor, createMemberIdentity } from '@crowd/data-access-layer'
import {
  getIdentitiesExistInOtherMembers as getIdentitiesExistInOthers,
  updateMemberAttributes,
} from '@crowd/data-access-layer/src/old/apps/members_enrichment_worker'
import { IAttributes, IMemberIdentity } from '@crowd/types'

import { svc } from '../../service'

export async function getIdentitiesExistInOtherMembers(
  excludeMemberId: string,
  identities: IMemberIdentity[],
): Promise<IMemberIdentity[]> {
  let rows: IMemberIdentity[] = []

  try {
    const db = svc.postgres.reader
    rows = await getIdentitiesExistInOthers(db, excludeMemberId, identities)
  } catch (err) {
    throw new Error(err)
  }

  return rows
}

export async function updateMemberWithEnrichmentData(
  memberId: string,
  identities: IMemberIdentity[],
  attributes?: IAttributes,
): Promise<void> {
  try {
    await svc.postgres.writer.connection().tx(async (tx) => {
      for (const identity of identities) {
        await createMemberIdentity(new PgPromiseQueryExecutor(tx), {
          memberId,
          platform: identity.platform,
          value: identity.value,
          type: identity.type,
          verified: identity.verified || false,
          source: 'enrichment',
        })
      }
      if (attributes) {
        await updateMemberAttributes(tx, memberId, attributes)
      }
    })
  } catch (err) {
    throw new Error(err)
  }
}

export async function mergeMembers(
  primaryMemberId: string,
  secondaryMemberId: string,
): Promise<void> {
  const res = await fetch(
    `${process.env['CROWD_API_SERVICE_URL']}/tenant/${DEFAULT_TENANT_ID}/member/${primaryMemberId}/merge`,
    {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${process.env['CROWD_API_SERVICE_USER_TOKEN']}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        memberToMerge: secondaryMemberId,
      }),
    },
  )

  if (res.status !== 200) {
    throw new Error(`Failed to merge member ${primaryMemberId} with ${secondaryMemberId}!`)
  }
}
