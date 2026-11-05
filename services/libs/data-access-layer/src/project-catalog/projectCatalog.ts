import { QueryExecutor } from '../queryExecutor'
import { prepareSelectColumns } from '../utils'
import {
  IDbProjectCatalogAnswer,
  IDbProjectCatalog,
  IDbProjectCatalogClassification,
  IDbProjectCatalogCreate,
  IDbProjectCatalogRef,
  IDbProjectCatalogUpdate,
  PROJECT_CATALOG_ACTIONS,
  PROJECT_CATALOG_BULK_PROVENANCES,
  ProjectCatalogAction,
  ProjectCatalogActionCounts,
  ProjectCatalogHumanProvenance,
  ProjectCatalogProvenance,
  IDbProjectCatalogReviewResolution,
  ProjectCatalogReviewDecision,
  IDbProjectCatalogReopened,
  IProjectCatalogReopenRequest,
  REVIEW_REJECTION_SKIP_REASON_PREFIX,
  reviewRejectionSkipReason,
} from './types'

const PROJECT_CATALOG_COLUMNS = [
  'id',
  'projectSlug',
  'repoName',
  'repoUrl',
  'source',
  'sourceUrl',
  'provenance',
  'action',
  'lfCriticalityScore',
  'evaluationResult',
  'evaluationReason',
  'evaluationAttempts',
  'evaluatedAt',
  'onboardedAt',
  'onboardingError',
  'skipReason',
  'requestText',
  'projectName',
  'classificationKind',
  'proposedAction',
  'pccProjectId',
  'classifiedAt',
  'reviewedAt',
  'reviewedBy',
  'reviewMessageTs',
  'reviewReason',
  'matchedProjectName',
  'matchedSegmentId',
  'decisionReason',
  'decisionNote',
  'requestedBy',
  'requestQuestion',
  'requestHints',
  'syncedAt',
  'createdAt',
  'updatedAt',
]

// A claim stamps onboardedAt while action stays 'onboard'; success always moves action to
// 'onboarded', so a claim older than the longest onboarding attempt belongs to a dead run.
const ONBOARDING_CLAIM_STALE_AFTER_MINUTES = 30
const ONBOARDING_CLAIM_STALE_AFTER = `${ONBOARDING_CLAIM_STALE_AFTER_MINUTES} minutes`
const ONBOARDING_CLAIM_OPEN = `("onboardedAt" IS NULL OR "onboardedAt" < NOW() - INTERVAL '${ONBOARDING_CLAIM_STALE_AFTER}')`

export function isProjectCatalogOnboardingPending(
  row: Pick<IDbProjectCatalog, 'action' | 'onboardedAt'>,
  now = Date.now(),
): boolean {
  if (row.action !== 'onboard') {
    return false
  }
  return (
    !row.onboardedAt ||
    Date.parse(row.onboardedAt) < now - ONBOARDING_CLAIM_STALE_AFTER_MINUTES * 60 * 1000
  )
}

export async function findProjectCatalogById(
  qx: QueryExecutor,
  id: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function findProjectCatalogByRepoUrl(
  qx: QueryExecutor,
  repoUrl: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE "repoUrl" = $(repoUrl)
    `,
    { repoUrl },
  )
}

export async function findProjectCatalogBySourceUrl(
  qx: QueryExecutor,
  sourceUrl: string,
): Promise<IDbProjectCatalog[]> {
  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE "sourceUrl" = $(sourceUrl)
    ORDER BY "createdAt" ASC, id ASC
    `,
    { sourceUrl },
  )
}

export async function findProjectCatalogByRepoUrls(
  qx: QueryExecutor,
  repoUrls: string[],
): Promise<IDbProjectCatalog[]> {
  if (repoUrls.length === 0) {
    return []
  }

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE "repoUrl" = ANY($(repoUrls)::text[])
    `,
    { repoUrls },
  )
}

export async function findProjectCatalogBySlug(
  qx: QueryExecutor,
  projectSlug: string,
): Promise<IDbProjectCatalog[]> {
  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE "projectSlug" = $(projectSlug)
    ORDER BY "createdAt" DESC
    `,
    { projectSlug },
  )
}

export async function findAllProjectCatalog(
  qx: QueryExecutor,
  options: { limit?: number; offset?: number } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, offset } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    ORDER BY "createdAt" DESC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { limit, offset },
  )
}

export async function findProjectCatalogPendingEvaluation(
  qx: QueryExecutor,
  options: { limit?: number; offset?: number; ids?: string[] } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, offset, ids } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE action = 'evaluate'
    ${ids !== undefined ? 'AND id = ANY($(ids)::uuid[])' : ''}
    ORDER BY
      (source = 'manual') DESC NULLS LAST,
      "lfCriticalityScore" DESC NULLS LAST,
      "createdAt" ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { limit, offset, ids },
  )
}

export async function findProjectCatalogPendingOnboarding(
  qx: QueryExecutor,
  options: { limit?: number; offset?: number; ids?: string[] } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, offset, ids } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE action = 'onboard' AND ${ONBOARDING_CLAIM_OPEN}
    ${ids !== undefined ? 'AND id = ANY($(ids)::uuid[])' : ''}
    ORDER BY
      (source = 'manual') DESC NULLS LAST,
      "lfCriticalityScore" DESC NULLS LAST,
      "createdAt" ASC,
      id ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    ${offset !== undefined ? 'OFFSET $(offset)' : ''}
    `,
    { limit, offset, ids },
  )
}

export async function findProjectCatalogPendingReview(
  qx: QueryExecutor,
  options: { limit?: number } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE action = 'review'
    ORDER BY "classifiedAt" ASC NULLS LAST, "createdAt" ASC, id ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    `,
    { limit },
  )
}

export async function findProjectCatalogReviewWithoutMessage(
  qx: QueryExecutor,
  options: { limit?: number; ids?: string[] } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, ids } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE action = 'review' AND "reviewMessageTs" IS NULL
    ${ids !== undefined ? 'AND id = ANY($(ids)::uuid[])' : ''}
    ORDER BY "classifiedAt" ASC NULLS LAST, "createdAt" ASC, id ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    `,
    { limit, ids },
  )
}

export interface IProjectCatalogOpenCounts {
  pending: number
  inReview: number
  asked: number
  failed: number
}

export async function countProjectCatalogOpenBySourceUrl(
  qx: QueryExecutor,
  sourceUrl: string,
): Promise<IProjectCatalogOpenCounts> {
  const row: { pending: string; inReview: string; asked: string; failed: string } =
    await qx.selectOne(
      `
    SELECT
      count(*) FILTER (WHERE action IN ('auto', 'evaluate', 'onboard')) AS pending,
      count(*) FILTER (WHERE action = 'review') AS "inReview",
      count(*) FILTER (WHERE action = 'ask') AS asked,
      count(*) FILTER (WHERE action IN ('error', 'unsure')) AS failed
    FROM "projectCatalog"
    WHERE "sourceUrl" = $(sourceUrl)
    `,
      { sourceUrl },
    )
  return {
    pending: Number(row.pending),
    inReview: Number(row.inReview),
    asked: Number(row.asked),
    failed: Number(row.failed),
  }
}

export async function findExistingProjectCatalogRepoUrls(
  qx: QueryExecutor,
  repoUrls: string[],
): Promise<Set<string>> {
  if (repoUrls.length === 0) {
    return new Set()
  }

  const rows: { repoUrl: string }[] = await qx.select(
    `
    SELECT "repoUrl"
    FROM "projectCatalog"
    WHERE "repoUrl" = ANY($(repoUrls)::text[])
    `,
    { repoUrls },
  )

  return new Set(rows.map((row) => row.repoUrl))
}

export async function promoteProjectCatalogProvenance(
  qx: QueryExecutor,
  provenance: ProjectCatalogHumanProvenance,
  refs: {
    repoUrl: string
    sourceUrl?: string | null
    organization?: string | null
    requestText?: string | null
    projectName?: string | null
    requestedBy?: string | null
  }[],
): Promise<number> {
  if (refs.length === 0) {
    return 0
  }

  const values = refs.map((ref) => ({
    repoUrl: ref.repoUrl,
    sourceUrl: ref.sourceUrl ?? null,
    organization: ref.organization ?? null,
    requestText: ref.requestText ?? null,
    projectName: ref.projectName ?? null,
    requestedBy: ref.requestedBy ?? null,
  }))

  return qx.result(
    `
    UPDATE "projectCatalog" pc
    SET "provenance" = $(provenance),
        "sourceUrl" = COALESCE(pc."sourceUrl", v."sourceUrl"),
        "requestHints" = CASE
          WHEN v."organization" IS NOT NULL AND pc."sourceUrl" IS NULL
            THEN COALESCE(pc."requestHints", '{}'::jsonb) || jsonb_build_object('organization', v."organization")
          ELSE pc."requestHints"
        END,
        "requestText" = CASE
          WHEN v."organization" IS NOT NULL AND pc."sourceUrl" IS NULL THEN COALESCE(v."requestText", pc."requestText")
          ELSE pc."requestText"
        END,
        "projectName" = CASE
          WHEN v."organization" IS NOT NULL AND pc."sourceUrl" IS NULL THEN COALESCE(v."projectName", pc."projectName")
          ELSE pc."projectName"
        END,
        "requestedBy" = CASE
          WHEN v."organization" IS NOT NULL AND pc."sourceUrl" IS NULL THEN COALESCE(v."requestedBy", pc."requestedBy")
          ELSE pc."requestedBy"
        END,
        "classifiedAt" = CASE
          WHEN v."organization" IS NOT NULL AND pc."sourceUrl" IS NULL AND pc."action" IN ('auto', 'evaluate') THEN NULL
          ELSE pc."classifiedAt"
        END,
        "evaluationAttempts" = CASE
          WHEN v."organization" IS NOT NULL AND pc."sourceUrl" IS NULL AND pc."action" IN ('auto', 'evaluate') THEN 0
          ELSE pc."evaluationAttempts"
        END,
        "updatedAt" = NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "repoUrl" text,
      "sourceUrl" text,
      "organization" text,
      "requestText" text,
      "projectName" text,
      "requestedBy" text
    )
    WHERE pc."repoUrl" = v."repoUrl"
      AND (pc."provenance" IS NULL OR pc."provenance" = ANY($(overridable)::text[]))
    `,
    {
      provenance,
      values: JSON.stringify(values),
      overridable: PROJECT_CATALOG_BULK_PROVENANCES,
    },
  )
}

export async function reopenRejectedProjectCatalog(
  qx: QueryExecutor,
  requests: IProjectCatalogReopenRequest[],
  decisionReasons: readonly string[],
): Promise<IDbProjectCatalogReopened[]> {
  if (requests.length === 0 || decisionReasons.length === 0) {
    return []
  }

  return qx.select(
    `
    WITH requested AS (
      SELECT *
      FROM jsonb_to_recordset($(values)::jsonb) AS v(
        "repoUrl" text,
        "sourceUrl" text,
        "requestText" text,
        "projectName" text,
        "requestedBy" text,
        "organization" text
      )
    ),
    rejected AS (
      SELECT pc.id, pc."repoUrl", pc."sourceUrl", pc."decisionReason"
      FROM "projectCatalog" pc
      JOIN requested v ON v."repoUrl" = pc."repoUrl"
      WHERE pc."action" = 'skip'
        AND pc."reviewedBy" IS NOT NULL
        AND pc."skipReason" LIKE $(rejectionPrefix) || '%'
        AND pc."decisionReason" = ANY($(decisionReasons)::text[])
      FOR UPDATE OF pc
    )
    UPDATE "projectCatalog" pc
    SET "action" = 'auto',
        "source" = 'slack-bot',
        "sourceUrl" = v."sourceUrl",
        "provenance" = 'slack-bot',
        "skipReason" = NULL,
        "requestText" = v."requestText",
        "projectName" = v."projectName",
        "requestedBy" = v."requestedBy",
        "requestHints" = CASE
          WHEN v."organization" IS NOT NULL THEN jsonb_build_object('organization', v."organization")
          ELSE NULL
        END,
        "classificationKind" = NULL,
        "proposedAction" = NULL,
        "pccProjectId" = NULL,
        "classifiedAt" = NULL,
        "reviewedAt" = NULL,
        "reviewedBy" = NULL,
        "reviewMessageTs" = NULL,
        "reviewReason" = NULL,
        "matchedProjectName" = NULL,
        "matchedSegmentId" = NULL,
        "decisionReason" = NULL,
        "decisionNote" = NULL,
        "requestQuestion" = NULL,
        "evaluationResult" = NULL,
        "evaluationReason" = NULL,
        "evaluationAttempts" = 0,
        "evaluatedAt" = NULL,
        "onboardingError" = NULL,
        "updatedAt" = NOW(),
        "syncedAt" = NOW()
    FROM rejected r
    JOIN requested v ON v."repoUrl" = r."repoUrl"
    WHERE pc.id = r.id
    RETURNING
      pc.id,
      pc."repoUrl",
      r."sourceUrl" AS "previousSourceUrl",
      r."decisionReason" AS "previousDecisionReason"
    `,
    {
      values: JSON.stringify(requests),
      rejectionPrefix: REVIEW_REJECTION_SKIP_REASON_PREFIX,
      decisionReasons,
    },
  )
}

export async function countProjectCatalog(qx: QueryExecutor): Promise<number> {
  const result = await qx.selectOne(
    `
    SELECT COUNT(*) AS count
    FROM "projectCatalog"
    `,
  )
  return parseInt(result.count, 10)
}

export async function countProjectCatalogByActions(
  qx: QueryExecutor,
): Promise<ProjectCatalogActionCounts> {
  const rows: { action: ProjectCatalogAction; count: number }[] = await qx.select(
    `
    SELECT action, COUNT(*)::int AS count
    FROM "projectCatalog"
    GROUP BY action
    `,
  )

  const counts = Object.fromEntries(
    PROJECT_CATALOG_ACTIONS.map((action) => [action, 0]),
  ) as ProjectCatalogActionCounts

  for (const row of rows) {
    if (row.action in counts) {
      counts[row.action] = row.count
    }
  }

  return counts
}

export async function countProjectCatalogByAction(
  qx: QueryExecutor,
  action: ProjectCatalogAction,
): Promise<number> {
  const result = await qx.selectOne(
    `
    SELECT COUNT(*) AS count
    FROM "projectCatalog"
    WHERE action = $(action)
    `,
    { action },
  )
  return parseInt(result.count, 10)
}

/**
 * Promotes 'auto' projects to 'evaluate', targeting a soft cap on the queue size.
 *
 * Uses a CTE to:
 *   1. Compute available slots (evaluateLimit − current 'evaluate' count) inline.
 *   2. Lock candidates with FOR UPDATE SKIP LOCKED — prevents double-promotion
 *      if two transactions run concurrently (e.g. simultaneous manual triggers).
 *      Note: concurrent calls can each compute the same slot count and promote
 *      up to evaluateLimit disjoint rows, so the cap is soft — the queue can
 *      overshoot by at most evaluateLimit per extra concurrent caller. In practice
 *      this doesn't happen: the schedule uses ScheduleOverlapPolicy.SKIP.
 *   3. Re-check action = 'auto' in the outer UPDATE to guard against rows whose
 *      state changed after the subquery snapshot (e.g. manual updates).
 *
 * Ordering (configurable via `sourcePriority`):
 *   1. Source priority — earlier position in the array = higher priority (unlisted = lowest)
 *   2. lfCriticalityScore DESC (NULLs last)
 *   3. createdAt ASC (stable tie-breaker)
 *
 * Returns the number of rows actually promoted.
 */
export async function promoteProjectsToEvaluate(
  qx: QueryExecutor,
  options: { evaluateLimit: number; sourcePriority: string[] },
): Promise<number> {
  const { evaluateLimit, sourcePriority } = options

  return qx.result(
    `
    WITH
      slots AS (
        SELECT GREATEST(0, $(evaluateLimit) - COUNT(*)) AS available
        FROM "projectCatalog"
        WHERE action = 'evaluate'
      ),
      candidates AS (
        SELECT pc.id
        FROM "projectCatalog" pc
        CROSS JOIN slots
        WHERE pc.action = 'auto'
          AND slots.available > 0
        ORDER BY
          COALESCE(ARRAY_POSITION($(sourcePriority)::text[], pc.source), 2147483647) ASC,
          pc."lfCriticalityScore" DESC NULLS LAST,
          pc."createdAt" ASC
        LIMIT (SELECT available FROM slots)
        FOR UPDATE SKIP LOCKED
      )
    UPDATE "projectCatalog"
    SET action = 'evaluate', "updatedAt" = NOW()
    FROM candidates
    WHERE "projectCatalog".id = candidates.id
      AND "projectCatalog".action = 'auto'
    `,
    { evaluateLimit, sourcePriority },
  )
}

export async function promoteProjectCatalogIdsToEvaluate(
  qx: QueryExecutor,
  ids: string[],
): Promise<number> {
  if (ids.length === 0) {
    return 0
  }

  return qx.result(
    `
    UPDATE "projectCatalog"
    SET action = 'evaluate', "updatedAt" = NOW()
    WHERE id = ANY($(ids)::uuid[])
      AND action = 'auto'
    `,
    { ids },
  )
}

export async function insertProjectCatalog(
  qx: QueryExecutor,
  data: IDbProjectCatalogCreate,
): Promise<IDbProjectCatalog> {
  return qx.selectOne(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "sourceUrl",
      "provenance",
      "action",
      "lfCriticalityScore",
      "requestText",
      "projectName",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      $(source),
      $(sourceUrl),
      $(provenance),
      $(action),
      $(lfCriticalityScore),
      $(requestText),
      $(projectName),
      NOW(),
      NOW(),
      NOW()
    )
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      projectSlug: data.projectSlug,
      repoName: data.repoName,
      repoUrl: data.repoUrl,
      source: data.source ?? null,
      sourceUrl: data.sourceUrl ?? null,
      provenance: data.provenance ?? null,
      action: data.action ?? 'auto',
      lfCriticalityScore: data.lfCriticalityScore ?? null,
      requestText: data.requestText ?? null,
      projectName: data.projectName ?? null,
    },
  )
}

export async function bulkInsertProjectCatalog(
  qx: QueryExecutor,
  items: IDbProjectCatalogCreate[],
): Promise<IDbProjectCatalogRef[]> {
  if (items.length === 0) {
    return []
  }

  const values = items.map((item) => ({
    projectSlug: item.projectSlug,
    repoName: item.repoName,
    repoUrl: item.repoUrl,
    source: item.source ?? null,
    sourceUrl: item.sourceUrl ?? null,
    provenance: item.provenance ?? null,
    action: item.action ?? 'auto',
    lfCriticalityScore: item.lfCriticalityScore ?? null,
    skipReason: item.skipReason ?? null,
    requestText: item.requestText ?? null,
    projectName: item.projectName ?? null,
    requestedBy: item.requestedBy ?? null,
    requestHints: item.requestHints ?? null,
  }))

  return qx.select(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "sourceUrl",
      "provenance",
      "action",
      "lfCriticalityScore",
      "skipReason",
      "requestText",
      "projectName",
      "requestedBy",
      "requestHints",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    SELECT
      v."projectSlug",
      v."repoName",
      v."repoUrl",
      v."source",
      v."sourceUrl",
      v."provenance",
      v."action",
      v."lfCriticalityScore"::double precision,
      v."skipReason",
      v."requestText",
      v."projectName",
      v."requestedBy",
      v."requestHints",
      NOW(),
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      "projectSlug" text,
      "repoName" text,
      "repoUrl" text,
      "source" text,
      "sourceUrl" text,
      "provenance" text,
      "action" text,
      "lfCriticalityScore" double precision,
      "skipReason" text,
      "requestText" text,
      "projectName" text,
      "requestedBy" text,
      "requestHints" jsonb
    )
    ON CONFLICT ("repoUrl") DO NOTHING
    RETURNING id, "repoUrl"
    `,
    { values: JSON.stringify(values) },
  )
}

export async function upsertProjectCatalog(
  qx: QueryExecutor,
  data: IDbProjectCatalogCreate,
): Promise<IDbProjectCatalog> {
  return qx.selectOne(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "sourceUrl",
      "provenance",
      "action",
      "lfCriticalityScore",
      "requestText",
      "projectName",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      $(source),
      $(sourceUrl),
      $(provenance),
      $(action),
      $(lfCriticalityScore),
      $(requestText),
      $(projectName),
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "source" = COALESCE(EXCLUDED."source", "projectCatalog"."source"),
      "sourceUrl" = COALESCE("projectCatalog"."sourceUrl", EXCLUDED."sourceUrl"),
      "provenance" = COALESCE("projectCatalog"."provenance", EXCLUDED."provenance"),
      "action" = CASE
        WHEN "projectCatalog"."action" IN ('review', 'onboard', 'onboarded', 'skip', 'unsure', 'error') THEN "projectCatalog"."action"
        WHEN EXCLUDED.action = 'evaluate' THEN 'evaluate'
        ELSE "projectCatalog"."action"
      END,
      "lfCriticalityScore" = COALESCE(EXCLUDED."lfCriticalityScore", "projectCatalog"."lfCriticalityScore"),
      "requestText" = COALESCE("projectCatalog"."requestText", EXCLUDED."requestText"),
      "projectName" = COALESCE("projectCatalog"."projectName", EXCLUDED."projectName"),
      "updatedAt" = NOW(),
      "syncedAt" = NOW()
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      projectSlug: data.projectSlug,
      repoName: data.repoName,
      repoUrl: data.repoUrl,
      source: data.source ?? null,
      sourceUrl: data.sourceUrl ?? null,
      provenance: data.provenance ?? null,
      action: data.action ?? 'auto',
      lfCriticalityScore: data.lfCriticalityScore ?? null,
      requestText: data.requestText ?? null,
      projectName: data.projectName ?? null,
    },
  )
}

export async function upsertProjectCatalogManualAction(
  qx: QueryExecutor,
  data: {
    projectSlug: string
    repoName: string
    repoUrl: string
    action: ProjectCatalogAction
    provenance?: ProjectCatalogProvenance | null
    projectName?: string | null
    requestText?: string | null
  },
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "provenance",
      "action",
      "projectName",
      "requestText",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      'manual',
      $(provenance),
      $(action),
      $(projectName),
      $(requestText),
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "source" = 'manual',
      "provenance" = COALESCE(EXCLUDED."provenance", "projectCatalog"."provenance"),
      "action" = EXCLUDED."action",
      "projectName" = COALESCE(EXCLUDED."projectName", "projectCatalog"."projectName"),
      "requestText" = COALESCE(EXCLUDED."requestText", "projectCatalog"."requestText"),
      "evaluatedAt" = CASE
        WHEN EXCLUDED."action" IN ('auto', 'evaluate') THEN NULL
        ELSE "projectCatalog"."evaluatedAt"
      END,
      "evaluationAttempts" = CASE
        WHEN EXCLUDED."action" IN ('auto', 'evaluate') THEN 0
        ELSE "projectCatalog"."evaluationAttempts"
      END,
      "classifiedAt" = CASE
        WHEN EXCLUDED."action" IN ('auto', 'evaluate') THEN NULL
        ELSE "projectCatalog"."classifiedAt"
      END,
      "onboardedAt" = CASE
        WHEN EXCLUDED."action" = 'onboard' THEN NULL
        ELSE "projectCatalog"."onboardedAt"
      END,
      "onboardingError" = CASE
        WHEN EXCLUDED."action" = 'onboard' THEN NULL
        ELSE "projectCatalog"."onboardingError"
      END,
      "updatedAt" = NOW(),
      "syncedAt" = NOW()
    WHERE "projectCatalog"."onboardedAt" IS NULL
      AND "projectCatalog"."action" NOT IN ('onboard', 'onboarded')
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      ...data,
      provenance: data.provenance ?? null,
      projectName: data.projectName ?? null,
      requestText: data.requestText ?? null,
    },
  )
}

// Slack-specific: unlike upsertProjectCatalogManualAction, also excludes an existing
// 'evaluate' row so two near-simultaneous Slack requests can't both trigger evaluateProject.
export async function claimProjectCatalogForSlackEvaluation(
  qx: QueryExecutor,
  data: {
    projectSlug: string
    repoName: string
    repoUrl: string
    provenance?: ProjectCatalogProvenance | null
  },
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    INSERT INTO "projectCatalog" (
      "projectSlug",
      "repoName",
      "repoUrl",
      "source",
      "provenance",
      "action",
      "createdAt",
      "updatedAt",
      "syncedAt"
    )
    VALUES (
      $(projectSlug),
      $(repoName),
      $(repoUrl),
      'manual',
      $(provenance),
      'evaluate',
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT ("repoUrl") DO UPDATE SET
      "projectSlug" = EXCLUDED."projectSlug",
      "repoName" = EXCLUDED."repoName",
      "source" = 'manual',
      "sourceUrl" = CASE
        WHEN "projectCatalog"."sourceUrl" ~ '^https://[^/]+.slack.com/archives/' THEN NULL
        ELSE "projectCatalog"."sourceUrl"
      END,
      "provenance" = COALESCE(EXCLUDED."provenance", "projectCatalog"."provenance"),
      "action" = 'evaluate',
      "evaluatedAt" = NULL,
      "classifiedAt" = NULL,
      "updatedAt" = NOW(),
      "syncedAt" = NOW()
    WHERE "projectCatalog"."onboardedAt" IS NULL
      AND "projectCatalog"."action" NOT IN ('onboard', 'onboarded', 'evaluate')
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    { ...data, provenance: data.provenance ?? null },
  )
}

// Held until the transaction ends; serializes the read-modify-write of one segment's GitHub
// integration across the scheduled and the bot-started onboarding workflows.
export async function lockProjectCatalogOnboardingTarget(
  qx: QueryExecutor,
  target: string,
): Promise<void> {
  await qx.result(
    `SELECT pg_advisory_xact_lock(hashtextextended('projectCatalogOnboarding:' || $(target)::text, 0))`,
    { target },
  )
}

// Claims a row queued for onboarding by stamping onboardedAt before the external call —
// the nightly worker's onboardedAt-truthy guard then treats it as already handled.
export async function claimProjectCatalogForOnboarding(
  qx: QueryExecutor,
  id: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET "onboardedAt" = NOW(), "updatedAt" = NOW()
    WHERE id = $(id) AND action = 'onboard' AND ${ONBOARDING_CLAIM_OPEN}
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    { id },
  )
}

export async function releaseProjectCatalogOnboardingClaim(
  qx: QueryExecutor,
  id: string,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "onboardedAt" = NULL, "updatedAt" = NOW()
    WHERE id = $(id) AND action = 'onboard' AND "onboardedAt" IS NOT NULL
    `,
    { id },
  )
}

export async function claimProjectCatalogForForcedOnboarding(
  qx: QueryExecutor,
  id: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET action = 'onboard', "onboardedAt" = NOW(), "updatedAt" = NOW()
    WHERE id = $(id)
      AND provenance = 'slack-bot'
      AND action IN ('skip', 'unsure')
      AND "evaluationResult" IS NOT NULL
      AND "evaluationResult" <> 'error'
      AND "onboardedAt" IS NULL
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    { id },
  )
}

export async function updateProjectCatalog(
  qx: QueryExecutor,
  id: string,
  data: IDbProjectCatalogUpdate,
): Promise<IDbProjectCatalog | null> {
  const setClauses: string[] = []
  const params: Record<string, unknown> = { id }

  if (data.projectSlug !== undefined) {
    setClauses.push('"projectSlug" = $(projectSlug)')
    params.projectSlug = data.projectSlug
  }
  if (data.repoName !== undefined) {
    setClauses.push('"repoName" = $(repoName)')
    params.repoName = data.repoName
  }
  if (data.repoUrl !== undefined) {
    setClauses.push('"repoUrl" = $(repoUrl)')
    params.repoUrl = data.repoUrl
  }
  if (data.source !== undefined) {
    setClauses.push('"source" = $(source)')
    params.source = data.source
  }
  if (data.action !== undefined) {
    setClauses.push('"action" = $(action)')
    params.action = data.action
  }
  if (data.lfCriticalityScore !== undefined) {
    setClauses.push('"lfCriticalityScore" = $(lfCriticalityScore)')
    params.lfCriticalityScore = data.lfCriticalityScore
  }
  if (data.evaluationResult !== undefined) {
    setClauses.push('"evaluationResult" = $(evaluationResult)')
    params.evaluationResult = data.evaluationResult
  }
  if (data.evaluationReason !== undefined) {
    setClauses.push('"evaluationReason" = $(evaluationReason)')
    params.evaluationReason = data.evaluationReason
  }
  if (data.syncedAt !== undefined) {
    setClauses.push('"syncedAt" = $(syncedAt)')
    params.syncedAt = data.syncedAt
  }
  if (data.evaluatedAt !== undefined) {
    setClauses.push('"evaluatedAt" = $(evaluatedAt)')
    params.evaluatedAt = data.evaluatedAt
  }
  if (data.onboardedAt !== undefined) {
    setClauses.push('"onboardedAt" = $(onboardedAt)')
    params.onboardedAt = data.onboardedAt
  }
  if (data.onboardingError !== undefined) {
    setClauses.push('"onboardingError" = $(onboardingError)')
    params.onboardingError = data.onboardingError
  }
  if (data.skipReason !== undefined) {
    setClauses.push('"skipReason" = $(skipReason)')
    params.skipReason = data.skipReason
  }
  if (data.requestText !== undefined) {
    setClauses.push('"requestText" = $(requestText)')
    params.requestText = data.requestText
  }
  if (data.projectName !== undefined) {
    setClauses.push('"projectName" = $(projectName)')
    params.projectName = data.projectName
  }
  if (data.matchedSegmentId !== undefined) {
    setClauses.push('"matchedSegmentId" = $(matchedSegmentId)')
    params.matchedSegmentId = data.matchedSegmentId
  }

  if (setClauses.length === 0) {
    return findProjectCatalogById(qx, id)
  }

  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET
      ${setClauses.join(',\n      ')},
      "updatedAt" = NOW()
    WHERE id = $(id)
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    params,
  )
}

export async function markProjectCatalogOnboardingFailed(
  qx: QueryExecutor,
  id: string,
  reason: string,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "action" = 'error', "onboardingError" = $(reason), "onboardedAt" = NULL, "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'onboard' AND ${ONBOARDING_CLAIM_OPEN}
    `,
    { id, reason },
  )
}

// Guarded like markProjectCatalogOnboardingFailed: a concurrent manual action wins.
// onboardingError is cleared in case this row was previously failed and requeued.
export async function markProjectCatalogOnboardingSkipped(
  qx: QueryExecutor,
  id: string,
  reason: string,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "action" = 'skip', "skipReason" = $(reason), "onboardingError" = NULL, "onboardedAt" = NULL, "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'onboard' AND ${ONBOARDING_CLAIM_OPEN}
    `,
    { id, reason },
  )
}

// Guarded like markProjectCatalogOnboardingFailed above: a manual request
// (POST /project-catalog) may have moved the row out of 'evaluate' while this
// evaluation was in flight — in that case the manual action wins and this
// write is a no-op.
export async function finalizeProjectCatalogEvaluation(
  qx: QueryExecutor,
  id: string,
  data: {
    action: ProjectCatalogAction
    evaluationResult: string
    evaluationReason: string | null
    reviewReason?: string | null
  },
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET
      "action" = $(action),
      "evaluationResult" = $(evaluationResult),
      "evaluationReason" = $(evaluationReason),
      "reviewReason" = COALESCE($(reviewReason), "reviewReason"),
      "evaluatedAt" = NOW(),
      "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'evaluate' AND "evaluatedAt" IS NULL
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    { id, ...data, reviewReason: data.reviewReason ?? null },
  )
}

// Guarded like finalizeProjectCatalogEvaluation. Explicitly nulls the verdict columns
// (clearing any stale prior verdict) so this skip stays out of the agent's skip alert.
export async function markProjectCatalogEvaluationFailed(
  qx: QueryExecutor,
  id: string,
  reason: string,
  expectedAttempts?: number,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "action" = 'error', "evaluationResult" = 'error', "evaluationReason" = $(reason),
        "evaluatedAt" = NOW(), "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'evaluate'
      AND ($(expectedAttempts) IS NULL OR "evaluationAttempts" = $(expectedAttempts))
    `,
    { id, reason, expectedAttempts: expectedAttempts ?? null },
  )
}

export async function requeueProjectCatalogEvaluation(
  qx: QueryExecutor,
  id: string,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET "action" = 'evaluate', "evaluationResult" = NULL, "evaluationReason" = NULL,
        "evaluationAttempts" = 0, "evaluatedAt" = NULL, "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'error' AND "evaluationResult" = 'error'
      AND "onboardingError" IS NULL
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    { id },
  )
}

export interface IDbProjectCatalogDeferral {
  reason: string
  expectedAttempts: number
  maxRuns: number
}

// The expected counter makes a redelivered deferral a no-op instead of a second attempt.
export async function deferProjectCatalogEvaluation(
  qx: QueryExecutor,
  id: string,
  { reason, expectedAttempts, maxRuns }: IDbProjectCatalogDeferral,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET "evaluationAttempts" = $(expectedAttempts) + 1, "evaluationReason" = $(reason),
        "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'evaluate' AND "evaluatedAt" IS NULL
      AND "evaluationAttempts" = $(expectedAttempts) AND $(expectedAttempts) + 1 < $(maxRuns)
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    { id, reason, expectedAttempts, maxRuns },
  )
}

export async function markProjectCatalogPreCheckSkipped(
  qx: QueryExecutor,
  id: string,
  reason: string,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "action" = 'skip', "skipReason" = $(reason), "evaluatedAt" = NOW(),
        "evaluationResult" = NULL, "evaluationReason" = NULL, "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'evaluate' AND "evaluatedAt" IS NULL
    `,
    { id, reason },
  )
}

// Guarded like finalizeProjectCatalogEvaluation: a manual action that moved the row
// out of 'evaluate' while classification was in flight wins and this write is a no-op.
export async function markProjectCatalogClassified(
  qx: QueryExecutor,
  id: string,
  data: IDbProjectCatalogClassification,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET
      "projectName" = $(projectName),
      "classificationKind" = $(classificationKind),
      "proposedAction" = $(proposedAction),
      "pccProjectId" = $(pccProjectId),
      "matchedProjectName" = $(matchedProjectName),
      "matchedSegmentId" = $(matchedSegmentId),
      "action" = $(action),
      "skipReason" = $(skipReason),
      "reviewReason" = $(reviewReason),
      "requestQuestion" = COALESCE($(requestQuestion)::jsonb, "requestQuestion"),
      "classifiedAt" = NOW(),
      "reviewedAt" = NULL,
      "reviewedBy" = NULL,
      "reviewMessageTs" = NULL,
      "decisionReason" = NULL,
      "decisionNote" = NULL,
      "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'evaluate' AND "classifiedAt" IS NULL
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      id,
      ...data,
      requestQuestion: data.requestQuestion ? JSON.stringify(data.requestQuestion) : null,
    },
  )
}

const QUESTION_PENDING = `"action" = 'ask' AND "requestQuestion" IS NOT NULL AND "requestQuestion"->>'messageTs' IS NULL`

export async function findProjectCatalogQuestionsWithoutMessage(
  qx: QueryExecutor,
  options: { limit?: number; ids?: string[] } = {},
): Promise<IDbProjectCatalog[]> {
  const { limit, ids } = options

  return qx.select(
    `
    SELECT ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    FROM "projectCatalog"
    WHERE ${QUESTION_PENDING}
    ${ids !== undefined ? 'AND id = ANY($(ids)::uuid[])' : ''}
    ORDER BY "classifiedAt" ASC NULLS LAST, "createdAt" ASC, id ASC
    ${limit !== undefined ? 'LIMIT $(limit)' : ''}
    `,
    { limit, ids },
  )
}

export async function lockProjectCatalogQuestion(qx: QueryExecutor, id: string): Promise<boolean> {
  const row = await qx.selectOneOrNone(
    `
    SELECT id
    FROM "projectCatalog"
    WHERE id = $(id) AND ${QUESTION_PENDING}
    FOR UPDATE
    `,
    { id },
  )
  return row !== null
}

export async function setProjectCatalogQuestionMessageTs(
  qx: QueryExecutor,
  id: string,
  messageTs: string,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "requestQuestion" = "requestQuestion" || jsonb_build_object('messageTs', $(messageTs)::text),
        "updatedAt" = NOW()
    WHERE id = $(id) AND ${QUESTION_PENDING}
    `,
    { id, messageTs },
  )
}

// The answer re-opens classification: hints are merged, the question keeps its record, and the
// row goes back to evaluate (or straight to review when the requester hands it to the team).
export async function answerProjectCatalogQuestion(
  qx: QueryExecutor,
  id: string,
  data: IDbProjectCatalogAnswer,
): Promise<IDbProjectCatalog | null> {
  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET
      "action" = $(action),
      "reviewReason" = $(reviewReason),
      "proposedAction" = COALESCE($(proposedAction), "proposedAction"),
      "requestHints" = COALESCE("requestHints", '{}'::jsonb) || COALESCE($(hints)::jsonb, '{}'::jsonb),
      "requestQuestion" = "requestQuestion" || jsonb_build_object(
        'answer', $(answer)::text, 'answeredBy', $(answeredBy)::text, 'answeredAt', NOW()
      ),
      "classifiedAt" = NULL,
      "evaluatedAt" = NULL,
      "evaluationResult" = NULL,
      "evaluationReason" = NULL,
      "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'ask'
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      id,
      action: data.action,
      reviewReason: data.reviewReason,
      proposedAction: data.proposedAction,
      hints: data.hints ? JSON.stringify(data.hints) : null,
      answer: data.answer,
      answeredBy: data.answeredBy,
    },
  )
}

interface IReviewOutcome {
  action: ProjectCatalogAction
  skipReason: string | null
  markOnboarded: boolean
  guard: string
}

// Rows that need a person (human_review, or no proposed action at all) can only be marked done;
// the others can only be approved.
function reviewOutcome(decision: ProjectCatalogReviewDecision, reviewedBy: string): IReviewOutcome {
  switch (decision) {
    case 'approve':
      return {
        action: 'onboard',
        skipReason: null,
        markOnboarded: false,
        guard: `"proposedAction" IS NOT NULL AND "proposedAction" <> 'human_review'`,
      }
    case 'done':
      return {
        action: 'onboarded',
        skipReason: null,
        markOnboarded: true,
        guard: `("proposedAction" IS NULL OR "proposedAction" = 'human_review')`,
      }
    case 'reject':
      return {
        action: 'skip',
        skipReason: reviewRejectionSkipReason(reviewedBy),
        markOnboarded: false,
        guard: 'TRUE',
      }
  }
}

export async function resolveProjectCatalogReview(
  qx: QueryExecutor,
  id: string,
  data: IDbProjectCatalogReviewResolution,
): Promise<IDbProjectCatalog | null> {
  const outcome = reviewOutcome(data.decision, data.reviewedBy)

  return qx.selectOneOrNone(
    `
    UPDATE "projectCatalog"
    SET
      "action" = $(action),
      "skipReason" = $(skipReason),
      "onboardedAt" = CASE WHEN $(markOnboarded) THEN NOW() ELSE "onboardedAt" END,
      "reviewedAt" = NOW(),
      "reviewedBy" = $(reviewedBy),
      "decisionReason" = $(decisionReason),
      "decisionNote" = $(decisionNote),
      "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'review' AND ${outcome.guard}
    RETURNING ${prepareSelectColumns(PROJECT_CATALOG_COLUMNS)}
    `,
    {
      id,
      action: outcome.action,
      skipReason: outcome.skipReason,
      markOnboarded: outcome.markOnboarded,
      reviewedBy: data.reviewedBy,
      decisionReason: data.decisionReason ?? null,
      decisionNote: data.decisionNote ?? null,
    },
  )
}

// Row lock held until the caller's transaction ends: concurrent scheduled and bot-started
// runs serialize on it, so only one of them posts the Slack card for the row.
export async function lockProjectCatalogReviewRequest(
  qx: QueryExecutor,
  id: string,
): Promise<boolean> {
  const row = await qx.selectOneOrNone(
    `
    SELECT id
    FROM "projectCatalog"
    WHERE id = $(id) AND "action" = 'review' AND "reviewMessageTs" IS NULL
    FOR UPDATE
    `,
    { id },
  )
  return row !== null
}

export async function setProjectCatalogReviewMessageTs(
  qx: QueryExecutor,
  id: string,
  reviewMessageTs: string,
): Promise<number> {
  return qx.result(
    `
    UPDATE "projectCatalog"
    SET "reviewMessageTs" = $(reviewMessageTs), "updatedAt" = NOW()
    WHERE id = $(id) AND "action" = 'review' AND "reviewMessageTs" IS NULL
    `,
    { id, reviewMessageTs },
  )
}

export async function setProjectCatalogSourceUrl(
  qx: QueryExecutor,
  id: string,
  sourceUrl: string,
): Promise<void> {
  await qx.selectNone(
    `
    UPDATE "projectCatalog"
    SET "sourceUrl" = $(sourceUrl), "updatedAt" = NOW()
    WHERE id = $(id) AND "sourceUrl" IS NULL
    `,
    { id, sourceUrl },
  )
}

export async function updateProjectCatalogSyncedAt(qx: QueryExecutor, id: string): Promise<void> {
  await qx.selectNone(
    `
    UPDATE "projectCatalog"
    SET "syncedAt" = NOW(), "updatedAt" = NOW()
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function deleteProjectCatalog(qx: QueryExecutor, id: string): Promise<number> {
  return qx.result(
    `
    DELETE FROM "projectCatalog"
    WHERE id = $(id)
    `,
    { id },
  )
}
