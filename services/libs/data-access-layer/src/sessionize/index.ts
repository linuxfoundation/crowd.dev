import { ConflictError } from '@crowd/common'
import { PlatformType, SegmentStatus } from '@crowd/types'

import { IFoundIntegration, findOrCreateAutoManagedIntegration } from '../integrations/autoManaged'
import { QueryExecutor } from '../queryExecutor'

export interface ISessionizeEventDecisionInput {
  eventKey: string
  eventName: string
  eventStart: string
  eventEnd: string
  cfpStart: string | null
  cfpEnd: string | null
  status: 'matched' | 'held'
  holdReason: string | null
  cventEventId: string | null
  catalogueSource: string | null
  segmentId: string | null
  rule: string | null
  ruleVersion: number
  evidence: Record<string, unknown>
}

export interface IStoredSessionizeEventMapping extends ISessionizeEventDecisionInput {
  reviewedAt: string | null
  lockedAt: string | null
  deactivatedAt: string | null
}

export type ISessionizeEvent = Pick<
  ISessionizeEventDecisionInput,
  'eventKey' | 'eventName' | 'eventStart' | 'eventEnd'
>

export type SessionizeDecisionOutcome = 'inserted' | 'updated' | 'unchanged' | 'protected'

interface IStoredDecisionDiff {
  isProtected: boolean
  isReviewed: boolean
  matchesAnotherSegment: boolean
  listingChanged: boolean
  targetChanged: boolean
  decisionChanged: boolean
}

export async function listSessionizeEventMappings(
  qx: QueryExecutor,
): Promise<IStoredSessionizeEventMapping[]> {
  return qx.select(
    `
      SELECT "eventKey", "eventName", "eventStart"::text AS "eventStart",
             "eventEnd"::text AS "eventEnd", "cfpStart", "cfpEnd", status, "holdReason",
             "cventEventId", "catalogueSource", "segmentId", rule, "ruleVersion", evidence,
             "reviewedAt",
             "lockedAt", "deactivatedAt"
      FROM integration."sessionizeEventMappings"
      ORDER BY "eventKey"
    `,
  )
}

export async function getSessionizeEvent(
  qx: QueryExecutor,
  eventKey: string,
): Promise<ISessionizeEvent | null> {
  return qx.selectOneOrNone(
    `
      SELECT "eventKey", "eventName", "eventStart"::text AS "eventStart",
             "eventEnd"::text AS "eventEnd"
      FROM integration."sessionizeEventMappings"
      WHERE "eventKey" = $(eventKey)
    `,
    { eventKey },
  )
}

async function refreshListing(
  tx: QueryExecutor,
  values: Record<string, unknown>,
  listingChanged: boolean,
): Promise<void> {
  await tx.result(
    `
      UPDATE integration."sessionizeEventMappings"
      SET "eventName" = $(eventName),
          "eventStart" = $(eventStart)::date,
          "eventEnd" = $(eventEnd)::date,
          "cfpStart" = $(cfpStart)::timestamp,
          "cfpEnd" = $(cfpEnd)::timestamp,
          "deactivatedAt" = NULL,
          "lastSeenAt" = now(),
          "updatedAt" = CASE WHEN $(listingChanged) THEN now() ELSE "updatedAt" END
      WHERE "eventKey" = $(eventKey)
    `,
    { ...values, listingChanged },
  )
}

export async function upsertSessionizeEventDecision(
  qx: QueryExecutor,
  decision: ISessionizeEventDecisionInput,
): Promise<SessionizeDecisionOutcome> {
  const values = { ...decision, evidence: JSON.stringify(decision.evidence) }

  return qx.tx(async (tx) => {
    await tx.select(
      `SELECT pg_advisory_xact_lock(hashtextextended('sessionizeEventMapping:' || $(eventKey), 0))`,
      { eventKey: decision.eventKey },
    )
    const stored: IStoredDecisionDiff | null = await tx.selectOneOrNone(
      `
        SELECT ("reviewedAt" IS NOT NULL OR "lockedAt" IS NOT NULL) AS "isProtected",
               "reviewedAt" IS NOT NULL AS "isReviewed",
               ($(status) = 'matched'
                 AND "segmentId" IS DISTINCT FROM $(segmentId)::uuid) AS "matchesAnotherSegment",
               ("eventName" IS DISTINCT FROM $(eventName)
                 OR "eventStart" IS DISTINCT FROM $(eventStart)::date
                 OR "eventEnd" IS DISTINCT FROM $(eventEnd)::date
                 OR "cfpStart" IS DISTINCT FROM $(cfpStart)::timestamp
                 OR "cfpEnd" IS DISTINCT FROM $(cfpEnd)::timestamp) AS "listingChanged",
               (status IS DISTINCT FROM $(status)
                 OR "holdReason" IS DISTINCT FROM $(holdReason)
                 OR "segmentId" IS DISTINCT FROM $(segmentId)::uuid
                 OR "cventEventId" IS DISTINCT FROM $(cventEventId)
                 OR "catalogueSource" IS DISTINCT FROM $(catalogueSource)) AS "targetChanged",
               (rule IS DISTINCT FROM $(rule)
                 OR "ruleVersion" IS DISTINCT FROM $(ruleVersion)::int
                 OR evidence IS DISTINCT FROM $(evidence)::jsonb) AS "decisionChanged"
        FROM integration."sessionizeEventMappings"
        WHERE "eventKey" = $(eventKey)
        FOR UPDATE
      `,
      values,
    )

    if (!stored) {
      await tx.result(
        `
          INSERT INTO integration."sessionizeEventMappings"
            ("eventKey", "eventName", "eventStart", "eventEnd", "cfpStart", "cfpEnd", status,
             "holdReason", "cventEventId", "catalogueSource", "segmentId", rule, "ruleVersion",
             evidence)
          VALUES
            ($(eventKey), $(eventName), $(eventStart)::date, $(eventEnd)::date,
             $(cfpStart)::timestamp, $(cfpEnd)::timestamp, $(status), $(holdReason),
             $(cventEventId), $(catalogueSource), $(segmentId)::uuid, $(rule), $(ruleVersion),
             $(evidence)::jsonb)
        `,
        values,
      )
      return 'inserted'
    }

    if (stored.isProtected) {
      await refreshListing(tx, values, stored.listingChanged)
      const needsReview = stored.isReviewed ? stored.matchesAnotherSegment : stored.targetChanged
      return needsReview ? 'protected' : 'unchanged'
    }

    if (!(stored.listingChanged || stored.targetChanged || stored.decisionChanged)) {
      await refreshListing(tx, values, false)
      return 'unchanged'
    }

    await tx.result(
      `
        UPDATE integration."sessionizeEventMappings"
        SET "eventName" = $(eventName),
            "eventStart" = $(eventStart)::date,
            "eventEnd" = $(eventEnd)::date,
            "cfpStart" = $(cfpStart)::timestamp,
            "cfpEnd" = $(cfpEnd)::timestamp,
            status = $(status),
            "holdReason" = $(holdReason),
            "cventEventId" = $(cventEventId),
            "catalogueSource" = $(catalogueSource),
            "segmentId" = $(segmentId)::uuid,
            rule = $(rule),
            "ruleVersion" = $(ruleVersion),
            evidence = $(evidence)::jsonb,
            "deactivatedAt" = NULL,
            "lastSeenAt" = now(),
            "updatedAt" = now()
        WHERE "eventKey" = $(eventKey)
      `,
      values,
    )
    return 'updated'
  })
}

export interface IDeactivatedSessionizeEvent {
  eventKey: string
  integrationId: string | null
}

export async function deactivateUnlistedSessionizeEvents(
  qx: QueryExecutor,
  listedEventKeys: string[],
): Promise<IDeactivatedSessionizeEvent[]> {
  if (listedEventKeys.length === 0) {
    throw new Error('Refusing to deactivate every Sessionize event against an empty listing!')
  }
  return qx.select(
    `
      UPDATE integration."sessionizeEventMappings"
      SET "deactivatedAt" = now()
      WHERE "deactivatedAt" IS NULL
        AND NOT ("eventKey" = ANY($(listedEventKeys)::text[]))
      RETURNING "eventKey", "integrationId"
    `,
    { listedEventKeys },
  )
}

export async function findOrCreateSessionizeIntegration(
  qx: QueryExecutor,
  segmentId: string,
): Promise<IFoundIntegration> {
  return qx.tx(async (tx) => {
    const subproject = await tx.selectOneOrNone(
      `SELECT 1 FROM public.segments WHERE id = $(segmentId) AND "grandparentSlug" IS NOT NULL`,
      { segmentId },
    )
    if (!subproject) {
      throw new Error(
        `Segment ${segmentId} is not a subproject, refusing a Sessionize integration!`,
      )
    }
    return findOrCreateAutoManagedIntegration(tx, PlatformType.SESSIONIZE, segmentId)
  })
}

export interface IOnboardableSessionizeEvent {
  eventKey: string
  eventName: string
  segmentId: string
  integrationId: string | null
}

export async function listOnboardableSessionizeEvents(
  qx: QueryExecutor,
): Promise<IOnboardableSessionizeEvent[]> {
  return qx.select(
    `
      SELECT "eventKey", "eventName", "segmentId", "integrationId"
      FROM integration."sessionizeEventMappings"
      WHERE status = 'matched' AND "deactivatedAt" IS NULL
      ORDER BY "eventKey"
    `,
  )
}

export async function markSessionizeEventOnboarded(
  qx: QueryExecutor,
  eventKey: string,
  segmentId: string,
  integrationId: string,
): Promise<void> {
  const found = await qx.result(
    `
      UPDATE integration."sessionizeEventMappings"
      SET "updatedAt" = CASE
            WHEN "lockedAt" IS NULL OR "integrationId" IS DISTINCT FROM $(integrationId)::uuid
            THEN now() ELSE "updatedAt"
          END,
          "integrationId" = $(integrationId),
          "lockedAt" = COALESCE("lockedAt", now())
      WHERE "eventKey" = $(eventKey)
        AND status = 'matched'
        AND "segmentId" = $(segmentId)
        AND "deactivatedAt" IS NULL
    `,
    { eventKey, segmentId, integrationId },
  )
  if (found === 0) {
    throw new Error(
      `Sessionize event ${eventKey} is no longer a live match of segment ${segmentId}, refusing to mark it onboarded!`,
    )
  }
}

export interface ISessionizeEventForReview {
  eventKey: string
  eventName: string
  eventStart: string
  eventEnd: string
  status: 'matched' | 'held' | 'excluded'
  holdReason: string | null
  cventEventId: string | null
  segmentId: string | null
  segmentName: string | null
  projectGroupName: string | null
  evidence: Record<string, unknown>
  reviewedAt: string | null
  lockedAt: string | null
}

export async function listSessionizeEventsForReview(
  qx: QueryExecutor,
): Promise<ISessionizeEventForReview[]> {
  return qx.select(
    `
      SELECT m."eventKey", m."eventName", m."eventStart"::text AS "eventStart",
             m."eventEnd"::text AS "eventEnd", m.status, m."holdReason", m."cventEventId",
             m."segmentId", s.name AS "segmentName", pg.name AS "projectGroupName",
             m.evidence, m."reviewedAt", m."lockedAt"
      FROM integration."sessionizeEventMappings" m
      LEFT JOIN segments s ON s.id = m."segmentId"
      LEFT JOIN segments pg
        ON pg.slug = s."grandparentSlug"
       AND pg."tenantId" = s."tenantId"
       AND pg."parentSlug" IS NULL
       AND pg."grandparentSlug" IS NULL
      WHERE m."deactivatedAt" IS NULL
        AND (m.status = 'held' OR m."reviewedAt" IS NOT NULL)
      ORDER BY m."eventStart" DESC, m."eventKey"
    `,
  )
}

async function applyReviewUpdate(
  qx: QueryExecutor,
  eventKey: string,
  action: string,
  query: string,
  params: Record<string, unknown>,
): Promise<void> {
  await qx.tx(async (tx) => {
    await tx.select(
      `SELECT pg_advisory_xact_lock(hashtextextended('sessionizeEventMapping:' || $(eventKey), 0))`,
      { eventKey },
    )
    const updated = await tx.result(query, { ...params, eventKey })
    if (updated === 0) {
      throw new ConflictError(`Sessionize event ${eventKey} can no longer be ${action}!`, {
        eventKey,
      })
    }
  })
}

export async function confirmSessionizeEventMapping(
  qx: QueryExecutor,
  input: { eventKey: string; segmentId: string; cventEventId: string | null; reviewedBy: string },
): Promise<void> {
  await applyReviewUpdate(
    qx,
    input.eventKey,
    'confirmed',
    `
      UPDATE integration."sessionizeEventMappings"
      SET status = 'matched',
          "segmentId" = $(segmentId)::uuid,
          "cventEventId" = $(cventEventId),
          rule = 'review',
          "reviewedBy" = $(reviewedBy),
          "reviewedAt" = now(),
          "updatedAt" = now()
      WHERE "eventKey" = $(eventKey)
        AND status = 'held'
        AND "reviewedAt" IS NULL
        AND "deactivatedAt" IS NULL
        AND EXISTS (
          SELECT 1 FROM segments
          WHERE id = $(segmentId)::uuid AND "grandparentSlug" IS NOT NULL AND status = $(active)
        )
    `,
    { ...input, active: SegmentStatus.ACTIVE },
  )
}

export async function ignoreSessionizeEvent(
  qx: QueryExecutor,
  input: { eventKey: string; reviewedBy: string },
): Promise<void> {
  await applyReviewUpdate(
    qx,
    input.eventKey,
    'ignored',
    `
      UPDATE integration."sessionizeEventMappings"
      SET status = 'excluded',
          "reviewedBy" = $(reviewedBy),
          "reviewedAt" = now(),
          "updatedAt" = now()
      WHERE "eventKey" = $(eventKey)
        AND status = 'held'
        AND "reviewedAt" IS NULL
        AND "deactivatedAt" IS NULL
    `,
    input,
  )
}

export async function restoreSessionizeEvent(qx: QueryExecutor, eventKey: string): Promise<void> {
  await applyReviewUpdate(
    qx,
    eventKey,
    'restored',
    `
      UPDATE integration."sessionizeEventMappings"
      SET status = 'held',
          "segmentId" = NULL,
          "cventEventId" = NULL,
          rule = NULL,
          "reviewedBy" = NULL,
          "reviewedAt" = NULL,
          "updatedAt" = now()
      WHERE "eventKey" = $(eventKey)
        AND "reviewedAt" IS NOT NULL
        AND "lockedAt" IS NULL
        AND "holdReason" IS NOT NULL
    `,
    {},
  )
}
