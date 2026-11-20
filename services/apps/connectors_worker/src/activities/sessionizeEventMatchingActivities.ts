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
import {
  SessionizeHeldReport,
  buildSessionizeHeldReport,
  sendSessionizeHeldReport,
} from '../sessionizeHeldReport'

export type SessionizeEventMatchingResult =
  | { summary: SessionizeMatchingSummary; heldReport: SessionizeHeldReport | null }
  | { skipped: 'not_configured' }

export async function runSessionizeEventMatching(): Promise<SessionizeEventMatchingResult> {
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
  log.info(
    { event: 'sessionize_event_matching_summary', ...summary },
    'sessionize event matching done',
  )
  return { summary, heldReport: report.changed ? report : null }
}

export async function postSessionizeHeldReport(report: SessionizeHeldReport): Promise<void> {
  if (!(await sendSessionizeHeldReport(report))) {
    throw new Error('Could not post the Sessionize held events report to Slack')
  }
}

export async function reportSessionizeEventMatchingFailure(message: string): Promise<void> {
  const sent = await sendSlackNotificationAsync(
    SlackChannel.CDP_INTEGRATIONS_ALERTS,
    SlackPersona.ERROR_REPORTER,
    'Sessionize event matching failed',
    message,
  )
  if (!sent) {
    svc.log.error({ message }, 'sessionize event matching: could not post the failure to Slack')
  }
}
