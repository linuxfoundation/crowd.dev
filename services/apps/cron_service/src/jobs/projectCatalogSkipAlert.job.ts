import CronTime from 'cron-time-generator'

import { IS_DEV_ENV, IS_PROD_ENV } from '@crowd/common'
import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  PROJECT_CATALOG_ACTIONS,
  ProjectCatalogAction,
  countProjectCatalogByActions,
} from '@crowd/data-access-layer/src/project-catalog'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
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

const CONTRADICTIONS_ONLY_REASONS = new Set<string>([ONBOARDED_REASON])

const MAX_ROWS_PER_SECTION = 25

const ACTION_LABELS: Record<ProjectCatalogAction, string> = {
  auto: 'Discovered',
  evaluate: 'Evaluate',
  onboard: 'Onboard',
  onboarded: 'Onboarded',
  skip: 'Skipped',
  unsure: 'Unsure',
  error: 'Error',
}

interface ISkipRow {
  repoUrl: string
  reason: string
  repoInCdp: boolean
  matchedProject: string | null
  matchedIsLf: boolean | null
  matchedDeletedAt: string | null
  suspicious: boolean | null
}

const job: IJobDefinition = {
  name: 'project-catalog-skip-alert',
  cronTime: IS_DEV_ENV ? CronTime.every(15).minutes() : CronTime.everyDayAt(17, 0),
  timeout: 10 * 60, // 10 minutes
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    ctx.log.info('Running project-catalog-skip-alert job...')

    const dbConnection = await getDbConnection(READ_DB_CONFIG(), 3, 0)

    let catalogCounts: Partial<Record<ProjectCatalogAction, number>> = {}

    try {
      catalogCounts = await countProjectCatalogByActions(pgpQx(dbConnection))
    } catch (err) {
      ctx.log.warn(err, 'Failed to fetch project catalog totals, omitting them from the report')
    }

    if (Object.keys(catalogCounts).length > 0) {
      const totalsText = PROJECT_CATALOG_ACTIONS.filter(
        (action) => (catalogCounts[action] ?? 0) > 0,
      )
        .map((action) => `${ACTION_LABELS[action]}: ${catalogCounts[action]}`)
        .join('\n')

      if (totalsText) {
        await sendSlackNotificationAsync(
          SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
          SlackPersona.SUMMARY_REPORTER,
          'Project Catalog Totals',
          totalsText,
        )
      }
    }

    const rows = await dbConnection.any<ISkipRow>(
      `
      WITH skipped AS (
        SELECT
          pc."repoUrl", COALESCE(pc."evaluationReason", '(no reason provided)') AS reason,
          CASE WHEN lower(regexp_replace(pc."repoUrl", '^https?://(www\\.)?([^/]+)/.*$', '\\2')) = 'github.com'
            THEN lower(regexp_replace(regexp_replace(pc."repoUrl",
              '^https?://(www\\.)?[^/]+/', ''), '(\\.git)?/*$', ''))
            ELSE regexp_replace(regexp_replace(pc."repoUrl",
              '^https?://(www\\.)?[^/]+/', ''), '(\\.git)?/*$', '')
          END                                                                 AS repo_path,
          lower(regexp_replace(regexp_replace(regexp_replace(pc."projectSlug",
            '[^a-zA-Z0-9-]+', '-', 'g'), '-+', '-', 'g'), '^-|-$', '', 'g')) AS derived_slug
        FROM "projectCatalog" pc
        WHERE pc.action = 'skip'
          AND pc."evaluationResult" = 'false'
          AND pc."evaluatedAt"::date = CURRENT_DATE
      ),
      repos_norm AS (
        SELECT
          repo_path,
          bool_or(true)                                              AS repo_exists,
          -- multiple unrelated LF projects can share a generic Gerrit path
          -- (e.g. "r/ci-management"); only trust the project when it's unambiguous
          CASE WHEN count(DISTINCT "insightsProjectId") = 1
            THEN (array_agg("insightsProjectId") FILTER (WHERE "insightsProjectId" IS NOT NULL))[1]
          END                                                         AS "insightsProjectId"
        FROM (
          SELECT id, "insightsProjectId",
            CASE WHEN lower(regexp_replace(url, '^https?://(www\\.)?([^/]+)/.*$', '\\2')) = 'github.com'
              THEN lower(regexp_replace(regexp_replace(url,
                '^https?://(www\\.)?[^/]+/', ''), '(\\.git)?/*$', ''))
              ELSE regexp_replace(regexp_replace(url,
                '^https?://(www\\.)?[^/]+/', ''), '(\\.git)?/*$', '')
            END                                                        AS repo_path
          FROM public.repositories
          WHERE "deletedAt" IS NULL
        ) x
        GROUP BY repo_path
      ),
      matched AS (
        SELECT
          s."repoUrl", s.reason, r.repo_exists,
          CASE WHEN ipr.id IS NOT NULL THEN ipr.id ELSE ips.id END               AS matched_id,
          CASE WHEN ipr.id IS NOT NULL THEN ipr.name ELSE ips.name END           AS matched_name,
          CASE WHEN ipr.id IS NOT NULL THEN ipr."isLF" ELSE ips."isLF" END       AS matched_is_lf,
          CASE WHEN ipr.id IS NOT NULL THEN ipr."deletedAt" ELSE ips."deletedAt" END AS matched_deleted_at
        FROM skipped s
        LEFT JOIN repos_norm r           ON r.repo_path = s.repo_path
        LEFT JOIN "insightsProjects" ipr ON ipr.id = r."insightsProjectId"
        LEFT JOIN "insightsProjects" ips ON ips.slug = s.derived_slug
      )
      SELECT
        m."repoUrl"          AS "repoUrl",
        m.reason              AS reason,
        COALESCE(m.repo_exists, false) AS "repoInCdp",
        m.matched_name         AS "matchedProject",
        m.matched_is_lf        AS "matchedIsLf",
        m.matched_deleted_at   AS "matchedDeletedAt",
        -- a soft-deleted insights project still counts as "exists in CDP"; its isLF
        -- flag doesn't, so LF-reason rows on it are unverifiable rather than flagged
        CASE m.reason
          WHEN $1 THEN (NOT COALESCE(m.repo_exists, false) AND m.matched_id IS NULL)
          WHEN $2 THEN CASE
            WHEN m.matched_id IS NULL          THEN true
            WHEN m.matched_deleted_at IS NOT NULL THEN NULL
            ELSE NOT COALESCE(m.matched_is_lf, false)
          END
          ELSE NULL
        END                                             AS suspicious
      FROM matched m
      ORDER BY suspicious DESC NULLS LAST, m.reason, m."repoUrl"
      `,
      [ONBOARDED_REASON, LF_REASON],
    )

    const flagged = rows.filter((row) => row.suspicious)
    const persona =
      flagged.length > 0 ? SlackPersona.WARNING_PROPAGATOR : SlackPersona.METRICS_REPORTER

    const sections: SlackMessageSection[] = [
      {
        title: '',
        text: [`Total: ${rows.length}`, `Contradicting: ${flagged.length}`].join('\n'),
      },
    ]

    const byReason = new Map<string, ISkipRow[]>()
    for (const row of rows) {
      const bucket = byReason.get(row.reason) ?? []
      bucket.push(row)
      byReason.set(row.reason, bucket)
    }

    for (const [reason, reasonRows] of byReason) {
      const contradictionsOnly = CONTRADICTIONS_ONLY_REASONS.has(reason)
      const listedRows = contradictionsOnly
        ? reasonRows.filter((row) => row.suspicious)
        : reasonRows

      if (contradictionsOnly && listedRows.length === 0) {
        continue
      }

      const visibleRows = listedRows.slice(0, MAX_ROWS_PER_SECTION)
      const lines = visibleRows.map((row) => formatLine(row))
      if (listedRows.length > visibleRows.length) {
        lines.push(`… and ${listedRows.length - visibleRows.length} more`)
      }

      sections.push({
        title: `Reason: "${reason}"`,
        text: [
          `Total: ${reasonRows.length}`,
          ...(contradictionsOnly ? [`Contradicting: ${listedRows.length}`] : []),
          ...lines,
        ].join('\n'),
      })
    }

    const precheckRows = await dbConnection.any<{ skipReason: string; total: string }>(
      `
      SELECT "skipReason", count(*) AS total
      FROM "projectCatalog"
      WHERE action = 'skip'
        AND "evaluationResult" IS NULL
        AND "skipReason" LIKE 'evaluation pre-check:%'
        AND "evaluatedAt"::date = CURRENT_DATE
      GROUP BY "skipReason"
      ORDER BY total DESC
      `,
    )

    if (precheckRows.length > 0) {
      sections.push({
        title: 'Deterministic pre-check (never reached the agent)',
        text: precheckRows.map((row) => `${row.skipReason}: ${row.total}`).join('\n'),
      })
    }

    await sendSlackNotificationAsync(
      SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
      persona,
      'Daily metrics for skipped projects',
      sections,
    )

    ctx.log.info(
      `Project catalog skip report processed: total=${rows.length}, flagged=${flagged.length}, catalog=${JSON.stringify(catalogCounts)}`,
    )
  },
}

function formatLine(row: ISkipRow): string {
  if (!row.suspicious) {
    return row.repoUrl
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
