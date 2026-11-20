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

export type SessionizeDecisionOutcome = 'inserted' | 'updated' | 'unchanged' | 'protected'

interface IStoredDecisionDiff {
  isProtected: boolean
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
             "cventEventId", "segmentId", rule, "ruleVersion", evidence, "reviewedAt",
             "lockedAt", "deactivatedAt"
      FROM integration."sessionizeEventMappings"
      ORDER BY "eventKey"
    `,
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
               ("eventName" IS DISTINCT FROM $(eventName)
                 OR "eventStart" IS DISTINCT FROM $(eventStart)::date
                 OR "eventEnd" IS DISTINCT FROM $(eventEnd)::date
                 OR "cfpStart" IS DISTINCT FROM $(cfpStart)::timestamp
                 OR "cfpEnd" IS DISTINCT FROM $(cfpEnd)::timestamp) AS "listingChanged",
               (status IS DISTINCT FROM $(status)
                 OR "holdReason" IS DISTINCT FROM $(holdReason)
                 OR "segmentId" IS DISTINCT FROM $(segmentId)::uuid
                 OR "cventEventId" IS DISTINCT FROM $(cventEventId)) AS "targetChanged",
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
             "holdReason", "cventEventId", "segmentId", rule, "ruleVersion", evidence)
          VALUES
            ($(eventKey), $(eventName), $(eventStart)::date, $(eventEnd)::date,
             $(cfpStart)::timestamp, $(cfpEnd)::timestamp, $(status), $(holdReason),
             $(cventEventId), $(segmentId)::uuid, $(rule), $(ruleVersion), $(evidence)::jsonb)
        `,
        values,
      )
      return 'inserted'
    }

    if (stored.isProtected) {
      await refreshListing(tx, values, stored.listingChanged)
      return stored.targetChanged ? 'protected' : 'unchanged'
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

export async function deactivateUnlistedSessionizeEvents(
  qx: QueryExecutor,
  listedEventKeys: string[],
): Promise<number> {
  if (listedEventKeys.length === 0) {
    throw new Error('Refusing to deactivate every Sessionize event against an empty listing!')
  }
  return qx.result(
    `
      UPDATE integration."sessionizeEventMappings"
      SET "deactivatedAt" = now()
      WHERE "deactivatedAt" IS NULL
        AND NOT ("eventKey" = ANY($(listedEventKeys)::text[]))
    `,
    { listedEventKeys },
  )
}
