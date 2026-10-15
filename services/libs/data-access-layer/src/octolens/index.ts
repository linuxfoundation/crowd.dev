import { PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export interface IOctolensKeywordMapping {
  id: string
  integrationId: string
  segmentId: string
  keywordId: number
  keyword: string
}

export async function findOctolensKeywordMapping(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<IOctolensKeywordMapping | null> {
  return qx.selectOneOrNone(
    `
      SELECT id, "integrationId", "segmentId", "keywordId", keyword
      FROM integration."octolensKeywordMappings"
      WHERE "integrationId" = $(integrationId)
        AND "keywordId" = $(keywordId)
        AND "deactivatedAt" IS NULL
    `,
    { integrationId, keywordId },
  )
}

export async function listOctolensKeywordMappings(
  qx: QueryExecutor,
  integrationId: string,
): Promise<IOctolensKeywordMapping[]> {
  return qx.select(
    `
      SELECT id, "integrationId", "segmentId", "keywordId", keyword
      FROM integration."octolensKeywordMappings"
      WHERE "integrationId" = $(integrationId) AND "deactivatedAt" IS NULL
      ORDER BY keyword
    `,
    { integrationId },
  )
}

export interface IUpsertOctolensKeywordMapping {
  integrationId: string
  segmentId: string
  keywordId: number
  keyword: string
}

// lastSeenAt moves on every call so gaps in discovery show up; updatedAt only on a real change.
export async function upsertOctolensKeywordMapping(
  qx: QueryExecutor,
  data: IUpsertOctolensKeywordMapping,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO integration."octolensKeywordMappings" AS m
        ("integrationId", "segmentId", "keywordId", keyword, "lastSeenAt")
      VALUES ($(integrationId), $(segmentId), $(keywordId), $(keyword), now())
      ON CONFLICT ("integrationId", "keywordId") DO UPDATE
      SET "segmentId" = EXCLUDED."segmentId",
          keyword = EXCLUDED.keyword,
          "deactivatedAt" = NULL,
          "lastSeenAt" = now(),
          "updatedAt" = CASE
            WHEN m."segmentId" IS DISTINCT FROM EXCLUDED."segmentId"
              OR m.keyword IS DISTINCT FROM EXCLUDED.keyword
              OR m."deactivatedAt" IS NOT NULL
            THEN now()
            ELSE m."updatedAt"
          END
    `,
    data,
  )
}

// An empty set deactivates nothing: a run that saw no mappings must never wipe them all.
export async function deactivateOctolensKeywordMappingsNotSeen(
  qx: QueryExecutor,
  integrationId: string,
  seenKeywordIds: number[],
): Promise<number> {
  if (seenKeywordIds.length === 0) {
    return 0
  }

  return qx.result(
    `
      UPDATE integration."octolensKeywordMappings"
      SET "deactivatedAt" = now(), "updatedAt" = now()
      WHERE "integrationId" = $(integrationId)
        AND "deactivatedAt" IS NULL
        AND "keywordId" <> ALL($(seenKeywordIds)::int[])
    `,
    { integrationId, seenKeywordIds },
  )
}

// The pg type parsers hand timestamptz back as strings.
type Timestamped<T, K extends keyof T> = Omit<T, K> & Record<K, string>

export interface IOctolensMappingDiscoveryState {
  integrationId: string
  activeMappings: number
  lastDiscoveredAt: Date | null
  integrationCreatedAt: Date
  checkedAt: Date
}

// A completed discovery run re-stamps every mapping it keeps active, but a run failing partway
// stamps only some, so the oldest active lastSeenAt is the last completed run.
export async function listOctolensMappingDiscoveryStates(
  qx: QueryExecutor,
): Promise<IOctolensMappingDiscoveryState[]> {
  const rows: (Omit<
    IOctolensMappingDiscoveryState,
    'lastDiscoveredAt' | 'integrationCreatedAt' | 'checkedAt'
  > & {
    lastDiscoveredAt: string | null
    integrationCreatedAt: string
    checkedAt: string
  })[] = await qx.select(
    `
      SELECT
        i.id AS "integrationId",
        count(m.id) FILTER (WHERE m."deactivatedAt" IS NULL)::int AS "activeMappings",
        COALESCE(
          min(m."lastSeenAt") FILTER (WHERE m."deactivatedAt" IS NULL),
          max(m."lastSeenAt")
        ) AS "lastDiscoveredAt",
        i."createdAt" AS "integrationCreatedAt",
        now() AS "checkedAt"
      FROM public.integrations i
      LEFT JOIN integration."octolensKeywordMappings" m ON m."integrationId" = i.id
      WHERE i.platform = $(platform) AND i."deletedAt" IS NULL
      GROUP BY i.id, i."createdAt"
      ORDER BY i.id
    `,
    { platform: PlatformType.OCTOLENS },
  )
  return rows.map((row) => ({
    ...row,
    lastDiscoveredAt: row.lastDiscoveredAt ? new Date(row.lastDiscoveredAt) : null,
    integrationCreatedAt: new Date(row.integrationCreatedAt),
    checkedAt: new Date(row.checkedAt),
  }))
}

export interface IDeactivatedOctolensKeywordMapping {
  integrationId: string
  keywordId: number
  keyword: string
  segmentId: string
  segmentName: string
  segmentSlug: string
  deactivatedAt: Date
}

export async function listOctolensKeywordMappingsDeactivatedBetween(
  qx: QueryExecutor,
  since: Date,
  until: Date,
): Promise<IDeactivatedOctolensKeywordMapping[]> {
  const rows: Timestamped<IDeactivatedOctolensKeywordMapping, 'deactivatedAt'>[] = await qx.select(
    `
      SELECT
        m."integrationId",
        m."keywordId",
        m.keyword,
        m."segmentId",
        s.name AS "segmentName",
        s.slug AS "segmentSlug",
        m."deactivatedAt"
      FROM integration."octolensKeywordMappings" m
      JOIN public.integrations i ON i.id = m."integrationId" AND i."deletedAt" IS NULL
      JOIN public.segments s ON s.id = m."segmentId"
      WHERE m."deactivatedAt" > $(since)
        AND m."deactivatedAt" <= $(until)
      ORDER BY m."integrationId", m."deactivatedAt", m.keyword
    `,
    { since, until },
  )
  return rows.map((row) => ({ ...row, deactivatedAt: new Date(row.deactivatedAt) }))
}
