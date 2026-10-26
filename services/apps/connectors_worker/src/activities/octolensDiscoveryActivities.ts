import {
  assertCredentialKind,
  getCredential,
  getManifest,
  isOctolensCredentialConfigured,
} from '@crowd/connectors'
import { resolveOctolensProjectSegmentId } from '@crowd/connectors/src/connectors/octolens/deps'
import { runMappingDiscovery } from '@crowd/connectors/src/connectors/octolens/mappingDiscovery'
import type {
  OctolensKeywordMappingInput,
  OctolensMappingDiscoveryResult,
} from '@crowd/connectors/src/connectors/octolens/mappingDiscovery'
import {
  reviveDeadLetteredSyncUnits,
  upsertSyncUnits,
} from '@crowd/data-access-layer/src/connectors'
import {
  findOrCreateOctolensAnchorIntegration,
  reconcileOctolensKeywordMappings,
} from '@crowd/data-access-layer/src/octolens'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { PlatformType } from '@crowd/types'

import { svc } from '../main'

const MENTION_POLL_SYNC_NAME = 'mentionPoll'

async function reconcileOctolensMappings(
  qx: QueryExecutor,
  mappings: OctolensKeywordMappingInput[],
): Promise<number> {
  const outcome = await reconcileOctolensKeywordMappings(qx, mappings)

  if (outcome.created.length > 0 || outcome.restored.length > 0) {
    svc.log.info(
      { created: outcome.created, restored: outcome.restored },
      'octolens discovery: subproject integrations created or restored',
    )
  }
  if (outcome.softDeleted.length > 0) {
    svc.log.info(
      { integrationIds: outcome.softDeleted },
      'octolens discovery: soft-deleted subproject integrations with no active keywords',
    )
  }

  return outcome.deactivated
}

export async function runOctolensDiscovery(): Promise<OctolensMappingDiscoveryResult> {
  if (!isOctolensCredentialConfigured()) {
    svc.log.info('octolens discovery skipped: CROWD_OCTOLENS_API_TOKEN is not set')
    return { complete: false, mappings: 0, deactivated: 0 }
  }

  const qx = dbStoreQx(svc.postgres.writer)

  // The anchor owns the single mention poll: Octolens's 500 req/hr budget is shared org-wide.
  const anchor = await findOrCreateOctolensAnchorIntegration(qx)
  const credential = await getCredential(qx, anchor.id)
  assertCredentialKind(credential, 'octolens-token')

  const channels = await getManifest(PlatformType.OCTOLENS).discover(credential)
  // emitEnabled is insert-only and must be true: false is shadow mode, which writes no activities.
  const syncUnitsInserted = await upsertSyncUnits(
    qx,
    channels.map((channel) => ({
      integrationId: anchor.id,
      platform: PlatformType.OCTOLENS,
      channelId: channel.channelId,
      channelName: channel.channelName,
      syncName: MENTION_POLL_SYNC_NAME,
      emitEnabled: true,
    })),
  )
  svc.log.info(
    { anchorIntegrationId: anchor.id, anchorCreated: anchor.created, syncUnitsInserted },
    'octolens discovery: ensured the mention poll sync unit (emitEnabled on insert)',
  )

  const result = await runMappingDiscovery({
    token: credential.data.token,
    log: svc.log,
    resolveProjectSegmentId: (slug) => resolveOctolensProjectSegmentId(qx, slug),
    reconcileMappings: (mappings) => reconcileOctolensMappings(qx, mappings),
  })

  // Discovery's own Octolens calls just succeeded with this token, so an auth dead-letter is stale.
  const revived = await reviveDeadLetteredSyncUnits(qx, anchor.id, MENTION_POLL_SYNC_NAME)
  if (revived > 0) {
    svc.log.info(
      { anchorIntegrationId: anchor.id },
      'octolens discovery: revived the dead-lettered mention poll sync unit',
    )
  }

  return result
}
