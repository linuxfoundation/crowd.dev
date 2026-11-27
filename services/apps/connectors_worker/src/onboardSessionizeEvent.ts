import {
  alignSyncUnitChannelName,
  findSyncUnitByChannelId,
  hasActiveSyncUnitsForChannelElsewhere,
  lockSyncUnitChannel,
  reviveDecommissionedSyncUnit,
  upsertSyncUnits,
} from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  findOrCreateSessionizeIntegration,
  listOnboardableSessionizeEvents,
  markSessionizeEventOnboarded,
} from '@crowd/data-access-layer/src/sessionize'
import type { Logger } from '@crowd/logging'
import { PlatformType } from '@crowd/types'

import { LIFECYCLE_STALE_LOCK_MS } from './githubLifecycle'

export const ONBOARDING_SPACING_SECONDS = 120

const EVENT_EXPORT_SYNC = 'eventExport'

export type OnboardSessionizeEventResult =
  | { outcome: 'created' }
  | { outcome: 'reactivated' }
  | {
      outcome: 'skipped'
      reason: 'owned_by_other_integration' | 'already_present'
      message: string
    }

export async function onboardSessionizeEvent(
  qx: QueryExecutor,
  integrationId: string,
  eventKey: string,
  eventName: string,
  initialDelaySeconds: number,
): Promise<OnboardSessionizeEventResult> {
  return qx.tx(async (txQx) => {
    await lockSyncUnitChannel(txQx, eventKey)
    const staleLockBefore = new Date(Date.now() - LIFECYCLE_STALE_LOCK_MS)
    if (
      await hasActiveSyncUnitsForChannelElsewhere(
        txQx,
        integrationId,
        eventKey,
        staleLockBefore,
        PlatformType.SESSIONIZE,
      )
    ) {
      return {
        outcome: 'skipped',
        reason: 'owned_by_other_integration',
        message: `units exist for sessionize event ${eventKey} under another integration`,
      }
    }

    const existing = await findSyncUnitByChannelId(
      txQx,
      integrationId,
      PlatformType.SESSIONIZE,
      eventKey,
      EVENT_EXPORT_SYNC,
    )
    if (existing) {
      await alignSyncUnitChannelName(txQx, integrationId, eventKey, eventName)
      if (existing.status === 'decommissioned') {
        await reviveDecommissionedSyncUnit(txQx, existing.id)
        return { outcome: 'reactivated' }
      }
      return {
        outcome: 'skipped',
        reason: 'already_present',
        message: `unit already exists for sessionize event ${eventKey}`,
      }
    }

    await upsertSyncUnits(txQx, [
      {
        integrationId,
        platform: PlatformType.SESSIONIZE,
        channelId: eventKey,
        channelName: eventName,
        syncName: EVENT_EXPORT_SYNC,
        emitEnabled: true,
        initialDelaySeconds,
      },
    ])
    return { outcome: 'created' }
  })
}

export interface SessionizeOnboardingSummary {
  integrationsCreated: number
  integrationsRestored: number
  unitsCreated: number
  unitsRevived: number
  skipped: Record<'owned_by_other_integration', number>
}

function emptyOnboardingSummary(): SessionizeOnboardingSummary {
  return {
    integrationsCreated: 0,
    integrationsRestored: 0,
    unitsCreated: 0,
    unitsRevived: 0,
    skipped: { owned_by_other_integration: 0 },
  }
}

export async function onboardMatchedSessionizeEvents(
  qx: QueryExecutor,
  log: Logger,
): Promise<SessionizeOnboardingSummary> {
  const summary = emptyOnboardingSummary()
  const integrationBySegment = new Map<string, string>()

  async function integrationFor(segmentId: string): Promise<string> {
    const known = integrationBySegment.get(segmentId)
    if (known) return known
    const integration = await findOrCreateSessionizeIntegration(qx, segmentId)
    if (integration.created) summary.integrationsCreated += 1
    if (integration.restored) summary.integrationsRestored += 1
    integrationBySegment.set(segmentId, integration.id)
    return integration.id
  }

  for (const event of await listOnboardableSessionizeEvents(qx)) {
    const integrationId = await integrationFor(event.segmentId)
    await qx.tx(async (txQx) => {
      const onboarded = await onboardSessionizeEvent(
        txQx,
        integrationId,
        event.eventKey,
        event.eventName,
        (summary.unitsCreated + 1) * ONBOARDING_SPACING_SECONDS,
      )
      if (onboarded.outcome === 'skipped' && onboarded.reason === 'owned_by_other_integration') {
        summary.skipped.owned_by_other_integration += 1
        log.warn(
          {
            event: 'sessionize_onboarding_skipped',
            eventKey: event.eventKey,
            integrationId,
            reason: onboarded.reason,
            message: onboarded.message,
          },
          'sessionize event onboarding skipped',
        )
        return
      }
      if (onboarded.outcome === 'created') summary.unitsCreated += 1
      if (onboarded.outcome === 'reactivated') summary.unitsRevived += 1
      await markSessionizeEventOnboarded(txQx, event.eventKey, integrationId)
    })
  }
  return summary
}
