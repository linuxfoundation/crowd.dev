import {
  alignSyncUnitChannelName,
  hasActiveSyncUnitsForChannelElsewhere,
  listSyncUnitChannelNames,
  lockSyncUnitChannel,
  upsertSyncUnits,
} from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { PlatformType } from '@crowd/types'

import { LIFECYCLE_STALE_LOCK_MS } from './githubLifecycle'

export type OnboardCredlyTemplateResult =
  | { outcome: 'created' }
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

    const existingNames = await listSyncUnitChannelNames(txQx, integrationId, badgeTemplateId)
    if (existingNames.length > 0) {
      await alignSyncUnitChannelName(txQx, integrationId, badgeTemplateId, badgeTemplateName)
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
