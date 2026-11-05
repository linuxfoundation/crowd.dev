import {
  alignSyncUnitChannelName,
  findSyncUnitByChannelId,
  hasActiveSyncUnitsForChannelElsewhere,
  lockSyncUnitChannel,
  reviveDecommissionedSyncUnit,
  upsertSyncUnits,
} from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { PlatformType } from '@crowd/types'

import { LIFECYCLE_STALE_LOCK_MS } from './githubLifecycle'

export type OnboardCredlyTemplateResult =
  | { outcome: 'created' }
  | { outcome: 'reactivated' }
  | {
      outcome: 'skipped'
      reason: 'owned_by_other_integration' | 'already_present'
      message: string
    }

export async function onboardCredlyTemplate(
  qx: QueryExecutor,
  integrationId: string,
  badgeTemplateId: string,
  badgeTemplateName: string,
  backfillWatermark: Record<string, unknown>,
): Promise<OnboardCredlyTemplateResult> {
  return qx.tx(async (txQx) => {
    await lockSyncUnitChannel(txQx, badgeTemplateId)
    const staleLockBefore = new Date(Date.now() - LIFECYCLE_STALE_LOCK_MS)
    if (
      await hasActiveSyncUnitsForChannelElsewhere(
        txQx,
        integrationId,
        badgeTemplateId,
        staleLockBefore,
        PlatformType.CREDLY,
      )
    ) {
      return {
        outcome: 'skipped',
        reason: 'owned_by_other_integration',
        message: `units exist for ${badgeTemplateName} under another integration`,
      }
    }

    const existing = await findSyncUnitByChannelId(
      txQx,
      integrationId,
      PlatformType.CREDLY,
      badgeTemplateId,
      'badgePoll',
    )
    if (existing) {
      await alignSyncUnitChannelName(txQx, integrationId, badgeTemplateId, badgeTemplateName)
      // A reappeared template leaves its old unit decommissioned: revive it instead of treating
      // it as already present, or the mapping would look healthy while nothing ever syncs again.
      if (existing.status === 'decommissioned') {
        await reviveDecommissionedSyncUnit(txQx, existing.id)
        return { outcome: 'reactivated' }
      }
      return {
        outcome: 'skipped',
        reason: 'already_present',
        message: `unit already exists for ${badgeTemplateName}`,
      }
    }

    await upsertSyncUnits(txQx, [
      {
        integrationId,
        platform: PlatformType.CREDLY,
        channelId: badgeTemplateId,
        channelName: badgeTemplateName,
        syncName: 'badgePoll',
        emitEnabled: true,
        watermark: backfillWatermark,
      },
    ])
    return { outcome: 'created' }
  })
}
