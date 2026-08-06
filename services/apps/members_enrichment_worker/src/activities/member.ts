import { CommonMemberService } from '@crowd/common_services'
import {
  MemberField,
  PgPromiseQueryExecutor,
  findMemberById,
  insertMemberIdentities,
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
  const db = svc.postgres.reader
  return getIdentitiesExistInOthers(db, excludeMemberId, identities)
}

export async function updateMemberWithEnrichmentData(
  memberId: string,
  identities: IMemberIdentity[],
  attributes?: IAttributes,
): Promise<void> {
  await svc.postgres.writer.connection().tx(async (tx) => {
    if (identities.length > 0) {
      await insertMemberIdentities(
        new PgPromiseQueryExecutor(tx),
        identities.map((identity) => ({
          memberId,
          platform: identity.platform,
          value: identity.value,
          type: identity.type,
          verified: identity.verified || false,
          source: 'enrichment',
        })),
      )
    }
    if (attributes) {
      await updateMemberAttributes(tx, memberId, attributes)
    }
  })
}

export async function mergeMembers(
  primaryMemberId: string,
  secondaryMemberId: string,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const memberService = new CommonMemberService(qx, svc.temporal, svc.log)

  try {
    await memberService.merge(primaryMemberId, secondaryMemberId)
  } catch (error) {
    svc.log.error({ err: error }, 'Failed to merge members')
    throw error
  }
}
