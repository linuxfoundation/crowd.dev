import { MemberField, findMemberById, pgpQx } from '@crowd/data-access-layer'
import { fetchMembersForEnrichment } from '@crowd/data-access-layer/src/old/apps/members_enrichment_worker'
import { MemberSyncService, OrganizationSyncService } from '@crowd/opensearch'
import {
  IEnrichableMember,
  IEnrichmentSourceQueryInput,
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
