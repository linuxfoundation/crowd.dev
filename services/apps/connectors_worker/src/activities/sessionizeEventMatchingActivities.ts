import { getPlatformCredential, isSessionizeCredentialConfigured } from '@crowd/connectors'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { listSessionizeEventMappings } from '@crowd/data-access-layer/src/sessionize'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'
import { SnowflakeClient } from '@crowd/snowflake'

import { svc } from '../main'
import { createCredentialHttp } from '../platformHttp'
import {
  SessionizeMatchingSummary,
  applySessionizeDecisions,
  computeSessionizeDecisions,
} from '../sessionizeEventMatching'
import { buildSessionizeHeldReport, postSessionizeHeldReport } from '../sessionizeHeldReport'

export async function runSessionizeEventMatching(): Promise<
  SessionizeMatchingSummary | { skipped: 'not_configured' }
> {
  const log = svc.log
  if (!isSessionizeCredentialConfigured()) {
    log.info('sessionize event matching skipped: the Sessionize organizer account is not set')
    return { skipped: 'not_configured' }
  }

  const qx = dbStoreQx(svc.postgres.writer)
  const http = await createCredentialHttp('sessionize', getPlatformCredential('sessionize'), log)
  const heldBefore = await listSessionizeEventMappings(qx)

  const snowflake = SnowflakeClient.fromEnv({ parentLog: log, maxConnections: 1 })
  let decisions: Awaited<ReturnType<typeof computeSessionizeDecisions>>
  try {
    decisions = await computeSessionizeDecisions({ http, catalogue: snowflake, qx, log })
  } finally {
    await snowflake.destroy()
  }

  const summary = await applySessionizeDecisions(qx, decisions, log)
  const report = buildSessionizeHeldReport(heldBefore, await listSessionizeEventMappings(qx))
  if (report.changed) {
    await postSessionizeHeldReport(report)
  }
  log.info(
    { event: 'sessionize_event_matching_summary', ...summary },
    'sessionize event matching done',
  )
  return summary
}

export async function reportSessionizeEventMatchingFailure(message: string): Promise<void> {
  try {
    await sendSlackNotificationAsync(
      SlackChannel.CDP_INTEGRATIONS_ALERTS,
      SlackPersona.ERROR_REPORTER,
      'Sessionize event matching failed',
      message,
    )
  } catch (err) {
    svc.log.error({ err }, 'sessionize event matching: could not post the failure to Slack')
  }
}
