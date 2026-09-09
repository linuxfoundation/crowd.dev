import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_PROD_ENV } from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  SlackChannel,
  SlackMessageSection,
  SlackPersona,
  sendSlackNotificationAsync,
} from '@crowd/slack'

import { IJobDefinition } from '../types'

// evaluationReason is free-text from an external API and can change wording anytime;
// only these two are DB-verifiable — the third ("not on GitHub") gave a false alarm on gcc/gcc.
const ONBOARDED_REASON = 'project is already onboarded'
const LF_REASON = 'project is already part of LF'

const MAX_ROWS_PER_SECTION = 25

interface ISkipRow {
  repoUrl: string
  reason: string
  repoInCdp: boolean
  matchedProject: string | null
  matchedIsLf: boolean | null
  suspicious: boolean | null
}

const job: IJobDefinition = {
  name: 'project-catalog-skip-alert',
  cronTime: IS_DEV_ENV ? CronTime.every(15).minutes() : CronTime.everyDayAt(8, 0),
  timeout: 10 * 60, // 10 minutes
  // cron_service schedules jobs in Europe/Berlin while evaluation runs at 04:00 UTC —
  // 08:00 Berlin stays safely after it (05:00-06:00 UTC) across both DST offsets.
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    ctx.log.info('Running project-catalog-skip-alert job...')

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)

    const rows = await dbConnection.any<ISkipRow>(
      `
      WITH skipped AS (
        SELECT
          pc."repoUrl", pc."evaluationReason" AS reason,
          lower(regexp_replace(regexp_replace(pc."repoUrl",
            '^https?://(www\\.)?github\\.com/', ''), '(\\.git)?/*$', ''))     AS repo_path,
          lower(regexp_replace(regexp_replace(regexp_replace(pc."projectSlug",
            '[^a-zA-Z0-9-]+', '-', 'g'), '-+', '-', 'g'), '^-|-$', '', 'g')) AS derived_slug
        FROM "projectCatalog" pc
        WHERE pc.action = 'skip'
          AND pc."evaluationResult" = 'false'
          AND pc."evaluatedAt"::date = CURRENT_DATE
      ),
      repos_norm AS (
        SELECT DISTINCT ON (repo_path) repo_path, id, "insightsProjectId"
        FROM (
          SELECT id, "insightsProjectId",
            lower(regexp_replace(regexp_replace(url,
              '^https?://(www\\.)?github\\.com/', ''), '(\\.git)?/*$', ''))  AS repo_path
          FROM public.repositories
          WHERE "deletedAt" IS NULL
        ) x
        ORDER BY repo_path, id
      )
      SELECT
        s."repoUrl"                                   AS "repoUrl",
        s.reason                                       AS reason,
        (r.id IS NOT NULL)                             AS "repoInCdp",
        COALESCE(ipr.name, ips.name)                   AS "matchedProject",
        COALESCE(ipr."isLF", ips."isLF")               AS "matchedIsLf",
        CASE s.reason
          WHEN $1 THEN (r.id IS NULL AND ips.id IS NULL)
          WHEN $2 THEN NOT COALESCE(ipr."isLF", ips."isLF", false)
          ELSE NULL
        END                                             AS suspicious
      FROM skipped s
      LEFT JOIN repos_norm r           ON r.repo_path = s.repo_path
      LEFT JOIN "insightsProjects" ipr ON ipr.id = r."insightsProjectId" AND ipr."deletedAt" IS NULL
      LEFT JOIN "insightsProjects" ips ON ips.slug = s.derived_slug      AND ips."deletedAt" IS NULL
      ORDER BY suspicious DESC NULLS LAST, s.reason, s."repoUrl"
      `,
      [ONBOARDED_REASON, LF_REASON],
    )

    const flagged = rows.filter((row) => row.suspicious)
    const persona =
      flagged.length > 0 ? SlackPersona.WARNING_PROPAGATOR : SlackPersona.INFO_NOTIFIER

    const sections: SlackMessageSection[] = [
      {
        title: 'Project Catalog Skip Summary',
        text: [
          `*Total skipped today:* ${rows.length}`,
          `*Flagged as contradicting the DB:* ${flagged.length}`,
        ].join('\n'),
      },
    ]

    const byReason = new Map<string, ISkipRow[]>()
    for (const row of rows) {
      const bucket = byReason.get(row.reason) ?? []
      bucket.push(row)
      byReason.set(row.reason, bucket)
    }

    for (const [reason, reasonRows] of byReason) {
      const visibleRows = reasonRows.slice(0, MAX_ROWS_PER_SECTION)
      const lines = visibleRows.map((row) => formatLine(row))
      if (reasonRows.length > visibleRows.length) {
        lines.push(`… and ${reasonRows.length - visibleRows.length} more`)
      }
      sections.push({
        title: `"${reason}" (${reasonRows.length})`,
        text: lines.join('\n'),
      })
    }

    await sendSlackNotificationAsync(
      SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
      persona,
      'Project Catalog Skip Report',
      sections,
    )

    ctx.log.info(
      `Project catalog skip report processed: total=${rows.length}, flagged=${flagged.length}`,
    )
  },
}

function formatLine(row: ISkipRow): string {
  if (!row.suspicious) {
    return `• ${row.repoUrl}`
  }

  const reasoning =
    row.reason === ONBOARDED_REASON
      ? 'not found in CDP at all'
      : row.matchedProject
        ? `matched to "${row.matchedProject}", which is not flagged as LF`
        : 'no matching project found in CDP'

  return `⚠️ *${row.repoUrl}* — ${reasoning}`
}

export default job
