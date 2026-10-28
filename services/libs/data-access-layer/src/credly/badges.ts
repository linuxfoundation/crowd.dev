import { QueryExecutor } from '../queryExecutor'

export type CredlyBadgeState = 'pending' | 'accepted' | 'rejected' | 'revoked' | 'replaced'

export interface ICredlyBadgeInput {
  badgeId: string
  badgeTemplateId: string
  badgeTemplateName: string
  badgeTemplateUrl: string | null
  state: CredlyBadgeState
  stateUpdatedAt: string | null
  issuedAt: string
  expiresAt: string | null
  replacementBadgeId: string | null
  replacesBadgeId: string | null
  segmentId: string
  integrationId: string
  recipientEmail: string
  issuedTo: string | null
}

export interface ICredlyBadgeRow extends ICredlyBadgeInput {
  expiredEmittedAt: string | null
  firstSeenAt: string
  lastSeenAt: string
}

const ROW_COLUMNS = `
  "badgeId", "badgeTemplateId", "badgeTemplateName", "badgeTemplateUrl", state, "stateUpdatedAt",
  "issuedAt", "expiresAt", "replacementBadgeId", "replacesBadgeId", "segmentId", "integrationId",
  "recipientEmail", "issuedTo", "expiredEmittedAt", "firstSeenAt", "lastSeenAt"`

export async function findCredlyBadges(
  qx: QueryExecutor,
  badgeIds: string[],
): Promise<ICredlyBadgeRow[]> {
  if (badgeIds.length === 0) return []
  return qx.select(
    `SELECT ${ROW_COLUMNS} FROM integration."credlyBadges" WHERE "badgeId" = ANY($(badgeIds)::text[])`,
    { badgeIds },
  )
}

// Segment and earner email stay those of the first sight for the badge's whole life: every later
// activity must land on the same member and segment as its issued-badge.
export async function upsertCredlyBadges(
  qx: QueryExecutor,
  badges: ICredlyBadgeInput[],
): Promise<void> {
  if (badges.length === 0) return
  const byId = new Map(badges.map((badge) => [badge.badgeId, badge]))
  const rows = [...byId.values()]
  await qx.result(
    `
      INSERT INTO integration."credlyBadges" AS b
        ("badgeId", "badgeTemplateId", "badgeTemplateName", "badgeTemplateUrl", state,
         "stateUpdatedAt", "issuedAt", "expiresAt", "replacementBadgeId", "replacesBadgeId",
         "segmentId", "integrationId", "recipientEmail", "issuedTo")
      SELECT * FROM unnest(
        $(badgeIds)::text[],
        $(badgeTemplateIds)::text[],
        $(badgeTemplateNames)::text[],
        $(badgeTemplateUrls)::text[],
        $(states)::text[],
        $(stateUpdatedAts)::timestamptz[],
        $(issuedAts)::timestamptz[],
        $(expiresAts)::timestamptz[],
        $(replacementBadgeIds)::text[],
        $(replacesBadgeIds)::text[],
        $(segmentIds)::uuid[],
        $(integrationIds)::uuid[],
        $(recipientEmails)::text[],
        $(issuedTos)::text[]
      )
      ON CONFLICT ("badgeId") DO UPDATE SET
        "badgeTemplateName" = EXCLUDED."badgeTemplateName",
        "badgeTemplateUrl" = EXCLUDED."badgeTemplateUrl",
        state = EXCLUDED.state,
        "stateUpdatedAt" = EXCLUDED."stateUpdatedAt",
        "expiresAt" = EXCLUDED."expiresAt",
        "replacementBadgeId" = EXCLUDED."replacementBadgeId",
        "replacesBadgeId" = EXCLUDED."replacesBadgeId",
        "issuedTo" = EXCLUDED."issuedTo",
        "lastSeenAt" = now(),
        "updatedAt" = CASE
          WHEN b.state IS DISTINCT FROM EXCLUDED.state
            OR b."stateUpdatedAt" IS DISTINCT FROM EXCLUDED."stateUpdatedAt"
            OR b."expiresAt" IS DISTINCT FROM EXCLUDED."expiresAt"
            OR b."replacementBadgeId" IS DISTINCT FROM EXCLUDED."replacementBadgeId"
            OR b."replacesBadgeId" IS DISTINCT FROM EXCLUDED."replacesBadgeId"
            OR b."issuedTo" IS DISTINCT FROM EXCLUDED."issuedTo"
          THEN now()
          ELSE b."updatedAt"
        END
    `,
    {
      badgeIds: rows.map((r) => r.badgeId),
      badgeTemplateIds: rows.map((r) => r.badgeTemplateId),
      badgeTemplateNames: rows.map((r) => r.badgeTemplateName),
      badgeTemplateUrls: rows.map((r) => r.badgeTemplateUrl),
      states: rows.map((r) => r.state),
      stateUpdatedAts: rows.map((r) => r.stateUpdatedAt),
      issuedAts: rows.map((r) => r.issuedAt),
      expiresAts: rows.map((r) => r.expiresAt),
      replacementBadgeIds: rows.map((r) => r.replacementBadgeId),
      replacesBadgeIds: rows.map((r) => r.replacesBadgeId),
      segmentIds: rows.map((r) => r.segmentId),
      integrationIds: rows.map((r) => r.integrationId),
      recipientEmails: rows.map((r) => r.recipientEmail),
      issuedTos: rows.map((r) => r.issuedTo),
    },
  )
}

export async function listDueCredlyExpiries(
  qx: QueryExecutor,
  now: Date,
  limit: number,
): Promise<ICredlyBadgeRow[]> {
  return qx.select(
    `
      SELECT ${ROW_COLUMNS}
      FROM integration."credlyBadges"
      WHERE state = 'accepted' AND "expiredEmittedAt" IS NULL AND "expiresAt" <= $(now)
      ORDER BY "expiresAt", "badgeId"
      LIMIT $(limit)
    `,
    { now, limit },
  )
}

export async function markCredlyExpiriesEmitted(
  qx: QueryExecutor,
  badgeIds: string[],
): Promise<number> {
  if (badgeIds.length === 0) return 0
  return qx.result(
    `
      UPDATE integration."credlyBadges"
      SET "expiredEmittedAt" = now(), "updatedAt" = now()
      WHERE "badgeId" = ANY($(badgeIds)::text[]) AND "expiredEmittedAt" IS NULL
    `,
    { badgeIds },
  )
}
