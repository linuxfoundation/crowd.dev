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
// only this one is DB-verifiable — the others ("not on GitHub", the old "already part
// of LF") either gave false alarms (gcc/gcc) or are no longer produced by the evaluator.
const ONBOARDED_REASON = 'project is already onboarded'

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
          -- host-agnostic matching is only safe within the known GitHub/Gerrit-mirror
          -- group; a generic multi-tenant host (gitlab.com, ...) keeps its own host
          -- in the key, since a shared org/repo path there is pure coincidence
          CASE WHEN pc.host = 'github.com' OR pc.host = 'review.opendev.org' OR pc.host LIKE 'gerrit.%'
            THEN 'gh:' || lower(pc.path)
            ELSE pc.host || '/' || pc.path
          END                                                                 AS repo_path,
          lower(regexp_replace(regexp_replace(regexp_replace(pc."projectSlug",
            '[^a-zA-Z0-9-]+', '-', 'g'), '-+', '-', 'g'), '^-|-$', '', 'g')) AS derived_slug
        FROM (
          SELECT pc.*,
            lower(regexp_replace(pc."repoUrl", '^https?://(www\\.)?([^/]+)/.*$', '\\2')) AS host,
            regexp_replace(regexp_replace(pc."repoUrl",
              '^https?://(www\\.)?[^/]+/', ''), '(\\.git)?/*$', '')                      AS path
          FROM "projectCatalog" pc
          WHERE pc.action = 'skip'
            AND pc."evaluationResult" = 'false'
            AND pc."evaluatedAt"::date = CURRENT_DATE
            -- reported via a dedicated per-repo alert instead, see CM-1792
            AND pc."provenance" IS DISTINCT FROM 'github-discussion'
        ) pc
      ),
      repos_norm AS (
        SELECT
          repo_path,
          bool_or(true)                                              AS repo_exists,
          -- multiple unrelated projects can share a generic Gerrit path
          -- (e.g. "r/ci-management"); only trust the match when it's unambiguous
          CASE WHEN count(DISTINCT "insightsProjectId") = 1
            THEN (array_agg("insightsProjectId") FILTER (WHERE "insightsProjectId" IS NOT NULL))[1]
          END                                                         AS "insightsProjectId"
        FROM (
          SELECT id, "insightsProjectId",
            CASE WHEN host = 'github.com' OR host = 'review.opendev.org' OR host LIKE 'gerrit.%'
              THEN 'gh:' || lower(path)
              ELSE host || '/' || path
            END                                                        AS repo_path
          FROM (
            SELECT id, "insightsProjectId",
              lower(regexp_replace(url, '^https?://(www\\.)?([^/]+)/.*$', '\\2')) AS host,
              regexp_replace(regexp_replace(url,
                '^https?://(www\\.)?[^/]+/', ''), '(\\.git)?/*$', '')             AS path
            FROM public.repositories
            WHERE "deletedAt" IS NULL
          ) h
        ) x
        GROUP BY repo_path
      ),
      matched AS (
        SELECT
          s."repoUrl", s.reason, r.repo_exists,
          CASE WHEN ipr.id IS NOT NULL THEN ipr.id ELSE ips.id END AS matched_id
        FROM skipped s
        LEFT JOIN repos_norm r           ON r.repo_path = s.repo_path
        LEFT JOIN "insightsProjects" ipr ON ipr.id = r."insightsProjectId"
        LEFT JOIN "insightsProjects" ips ON ips.slug = s.derived_slug
      )
      SELECT
        m."repoUrl"          AS "repoUrl",
        m.reason              AS reason,
        COALESCE(m.repo_exists, false) AS "repoInCdp",
        CASE m.reason
          WHEN $1 THEN (NOT COALESCE(m.repo_exists, false) AND m.matched_id IS NULL)
          ELSE NULL
        END                                             AS suspicious
      FROM matched m
      ORDER BY suspicious DESC NULLS LAST, m.reason, m."repoUrl"
      `,
      [ONBOARDED_REASON],
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
        -- reported via a dedicated per-repo alert instead, see CM-1792
        AND "provenance" IS DISTINCT FROM 'github-discussion'
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

  return `⚠️ *${row.repoUrl}* — not found in CDP at all`
}

export default job
