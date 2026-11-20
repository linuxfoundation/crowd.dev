import { getPlatformCredential, isSessionizeCredentialConfigured } from '@crowd/connectors'
import { createSessionizeSessionHttp } from '@crowd/connectors/src/connectors/sessionize/sessionHttp'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

import { svc } from '../main'
import {
  SessionizeMatchingSummary,
  applySessionizeDecisions,
  computeSessionizeDecisions,
} from '../sessionizeEventMatching'

export type SessionizeEventMatchingResult =
  | SessionizeMatchingSummary
  | { skipped: 'not_configured' }

export async function runSessionizeEventMatching(): Promise<SessionizeEventMatchingResult> {
  const log = svc.log
  if (!isSessionizeCredentialConfigured()) {
    log.info('sessionize event matching skipped: the Sessionize organizer account is not set')
    return { skipped: 'not_configured' }
  }

  const region =
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION ||
    process.env.CROWD_SNOWFLAKE_S3_REGION
  if (!region) {
    throw new Error(
      'sessionize event matching needs CROWD_SNOWFLAKE_S3_REGION or AWS_REGION to load the Snowflake client',
    )
  }
  process.env.AWS_REGION ||= region

  const qx = dbStoreQx(svc.postgres.writer)
  const http = createSessionizeSessionHttp(getPlatformCredential('sessionize'))
  // Loaded per run: with dd-trace active, snowflake-sdk without an AWS region crashes the process
  // on load (unhandled "Region is missing"), so it must never load at worker startup.
  const { SnowflakeClient } = await import('@crowd/snowflake')
  const snowflake = SnowflakeClient.fromEnv({ parentLog: log, maxConnections: 1 })
  let decisions: Awaited<ReturnType<typeof computeSessionizeDecisions>>
  try {
    decisions = await computeSessionizeDecisions({ http, catalogue: snowflake, qx, log })
  } finally {
    await snowflake.destroy()
  }

  const summary = await applySessionizeDecisions(qx, decisions, log)
  log.info(
    { event: 'sessionize_event_matching_summary', ...summary },
    'sessionize event matching done',
  )
  return summary
}

export async function reportSessionizeEventMatchingFailure(message: string): Promise<void> {
  const sent = await sendSlackNotificationAsync(
    SlackChannel.CDP_INTEGRATIONS_ALERTS,
    SlackPersona.ERROR_REPORTER,
    'Sessionize event matching failed',
    message,
  )
  if (!sent) {
    throw new Error('Could not post the Sessionize event matching failure to Slack')
  }
}
