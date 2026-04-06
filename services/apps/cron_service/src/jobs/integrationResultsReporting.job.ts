import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_PROD_ENV } from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  SlackChannel,
  SlackMessageSection,
  SlackPersona,
  sendSlackNotificationAsync,
} from '@crowd/slack'
import { IntegrationResultState } from '@crowd/types'

import { IJobDefinition } from '../types'

interface IResultStateCount {
  state: string
  count: number
}

interface IErrorGroup {
  errorMessage: string
  location: string
  message: string
  count: number
  avgRetries: number
  maxRetries: number
  oldest: Date
  newest: Date
  platforms: string | null
}

const job: IJobDefinition = {
  name: 'integration-results-reporting',
  cronTime: IS_DEV_ENV ? CronTime.every(15).minutes() : CronTime.everyDayAt(8, 30),
  timeout: 10 * 60, // 10 minutes
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    ctx.log.info('Running integration-results-reporting job...')

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)

    // Count results per state
    const stateCounts = await dbConnection.any<IResultStateCount>(
      `SELECT state, count(*)::int AS count FROM integration.results GROUP BY state ORDER BY count DESC`,
    )

    const countByState: Record<string, number> = {}
    for (const row of stateCounts) {
      countByState[row.state] = row.count
    }

    const pending = countByState[IntegrationResultState.PENDING] ?? 0
    const processing = countByState[IntegrationResultState.PROCESSING] ?? 0
    const processed = countByState[IntegrationResultState.PROCESSED] ?? 0
    const delayed = countByState[IntegrationResultState.DELAYED] ?? 0
    const errorCount = countByState[IntegrationResultState.ERROR] ?? 0
    const total = pending + processing + processed + delayed + errorCount

    // How many delayed results are overdue (i.e. should already be processed)
    const overdueDelayed = (
      await dbConnection.one<{ count: number }>(
        `SELECT count(*)::int AS count FROM integration.results WHERE state = 'delayed' AND "delayedUntil" < now()`,
      )
    ).count

    // Break down errors by errorMessage + location, enriched with platform info
    const errorGroups = await dbConnection.any<IErrorGroup>(
      `
      SELECT
        COALESCE(r.error->>'errorMessage', '[no errorMessage]') AS "errorMessage",
        COALESCE(r.error->>'location',     '[no location]')     AS location,
        COALESCE(r.error->>'message',      '[no message]')      AS message,
        count(*)::int                                           AS count,
        round(avg(r.retries), 1)::float                         AS "avgRetries",
        max(r.retries)::int                                     AS "maxRetries",
        min(r."createdAt")                                      AS oldest,
        max(r."updatedAt")                                      AS newest,
        string_agg(DISTINCT i.platform, ', ' ORDER BY i.platform) AS platforms
      FROM integration.results r
      LEFT JOIN integrations i ON i.id = r."integrationId"
      WHERE r.state = 'error'
      GROUP BY
        r.error->>'errorMessage',
        r.error->>'location',
        r.error->>'message'
      ORDER BY count DESC
      LIMIT 20
      `,
    )

    const sections: SlackMessageSection[] = []

    sections.push({
      title: 'Integration Results Summary',
      text: [
        `*Total:* ${total.toLocaleString()}`,
        '',
        `⏳ Pending:    *${pending.toLocaleString()}*`,
        `⚙️ Processing: *${processing.toLocaleString()}*`,
        `✅ Processed:  *${processed.toLocaleString()}*`,
        `🕐 Delayed:    *${delayed.toLocaleString()}*${overdueDelayed > 0 ? ` (${overdueDelayed.toLocaleString()} overdue)` : ''}`,
        `❌ Error:      *${errorCount.toLocaleString()}*`,
      ].join('\n'),
    })

    if (errorCount > 0 && errorGroups.length > 0) {
      const lines: string[] = [
        `Top ${errorGroups.length} error group${errorGroups.length !== 1 ? 's' : ''} out of *${errorCount.toLocaleString()}* total errors:`,
        '',
      ]

      for (const group of errorGroups) {
        const oldestHoursAgo = Math.round(
          (Date.now() - new Date(group.oldest).getTime()) / 3_600_000,
        )
        const newestHoursAgo = Math.round(
          (Date.now() - new Date(group.newest).getTime()) / 3_600_000,
        )
        const ageLabel =
          oldestHoursAgo === newestHoursAgo
            ? formatHoursAgo(oldestHoursAgo)
            : `${formatHoursAgo(newestHoursAgo)} – ${formatHoursAgo(oldestHoursAgo)}`

        lines.push(
          `• *${group.count}x* \`${group.errorMessage}\``,
          `  _Location:_ \`${group.location}\` | _retries avg/max:_ ${group.avgRetries}/${group.maxRetries}${group.platforms ? ` | _platforms:_ \`${group.platforms}\`` : ''}`,
          `  _Age:_ ${ageLabel}`,
          `  _Detail:_ ${group.message}`,
          '',
        )
      }

      sections.push({
        title: `Error Breakdown (top ${errorGroups.length})`,
        text: lines.join('\n'),
      })
    }

    const persona = errorCount > 0 ? SlackPersona.WARNING_PROPAGATOR : SlackPersona.INFO_NOTIFIER

    await sendSlackNotificationAsync(
      SlackChannel.CDP_INTEGRATIONS_ALERTS,
      persona,
      'Integration Results Daily Report',
      sections,
    )

    ctx.log.info(
      `Integration results report sent: pending=${pending}, delayed=${delayed} (${overdueDelayed} overdue), errors=${errorCount}`,
    )
  },
}

function formatHoursAgo(hours: number): string {
  if (hours < 1) return 'just now'
  if (hours < 24) return `${hours}h ago`
  return `${Math.round(hours / 24)}d ago`
}

export default job
