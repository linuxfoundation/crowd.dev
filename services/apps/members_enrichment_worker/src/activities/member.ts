import {
  MemberField,
  PgPromiseQueryExecutor,
  createMemberIdentity,
  findMemberById,
  pgpQx,
} from '@crowd/data-access-layer'
import {
  fetchMembersForEnrichment,
  getIdentitiesExistInOtherMembers as getIdentitiesExistInOthers,
  updateMemberAttributes,
} from '@crowd/data-access-layer/src/old/apps/members_enrichment_worker'
import { MemberSyncService, OrganizationSyncService } from '@crowd/opensearch'
import {
  IAttributes,
  IEnrichableMember,
  IEnrichmentSourceQueryInput,
  IMemberIdentity,
  MemberEnrichmentSource,
} from '@crowd/types'

import { EnrichmentSourceServiceFactory } from '../factory'
import { svc } from '../service'

export async function getEnrichableMembers(
  limit: number,
  sources: MemberEnrichmentSource[],
): Promise<IEnrichableMember[]> {
  const sourceInputs: IEnrichmentSourceQueryInput<MemberEnrichmentSource>[] = sources.map((s) => {
    const srv = EnrichmentSourceServiceFactory.getEnrichmentSourceService(s, svc.log)

    if (!srv.neverReenrich && srv.cacheObsoleteAfterSeconds == null) {
      throw new Error(
        `"${s}" must define cacheObsoleteAfterSeconds if neverReenrich is not enabled`,
      )
    }

    return {
      source: s,
      cacheObsoleteAfterSeconds: srv.cacheObsoleteAfterSeconds,
      enrichableBySql: srv.enrichableBySql,
      neverReenrich: srv.neverReenrich,
    }
  })

  return fetchMembersForEnrichment(svc.postgres.reader, limit, sourceInputs)
}

export async function getMemberById(memberId: string): Promise<boolean> {
  const qx = pgpQx(svc.postgres.reader.connection())
  const member = await findMemberById(qx, memberId, [MemberField.ID])
  return !!member
}

const memberSyncService = new MemberSyncService(
  svc.redis,
  svc.postgres.writer,
  svc.opensearch,
  svc.log,
)

const organizationSyncService = new OrganizationSyncService(
  svc.postgres.writer,
  svc.opensearch,
  svc.log,
)

export async function syncMembersToOpensearch(input: string): Promise<void> {
  await memberSyncService.syncMembers(input)
}

export async function syncOrganizationsToOpensearch(input: string[]): Promise<void> {
  await organizationSyncService.syncOrganizations(input)
}

export async function getIdentitiesExistInOtherMembers(
  excludeMemberId: string,
  identities: IMemberIdentity[],
): Promise<IMemberIdentity[]> {
  let rows: IMemberIdentity[] = []

  try {
    const db = svc.postgres.reader
    rows = await getIdentitiesExistInOthers(db, excludeMemberId, identities)
  } catch (err) {
    throw err
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
    throw err
  }
}

export async function mergeMembers(
  primaryMemberId: string,
  secondaryMemberId: string,
): Promise<void> {
  const res = await fetch(
    `${process.env['CROWD_API_SERVICE_URL']}/member/${primaryMemberId}/merge`,
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
    const body = await res.text()
    throw new Error(
      `Failed to merge member ${primaryMemberId} with ${secondaryMemberId}! Status: ${res.status}, Response: ${body}`,
    )
  }
}
