import { PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { OWNS_NO_ACTIVE_SYNC_UNITS } from './index'

const OCTOLENS_SUBPROJECT_INTEGRATION = `
  i.platform = $(platform)
  AND i."segmentId" IS NOT NULL
  AND ${OWNS_NO_ACTIVE_SYNC_UNITS}
`

export interface IOctolensMappingDiscoveryHealth {
  activeMappings: number
  integrations: number
  lastDiscoveredAt: Date
  checkedAt: Date
}

// Discovery stamps every mapping it keeps in one transaction, so the newest lastSeenAt is its last
// complete run.
export async function getOctolensMappingDiscoveryHealth(
  qx: QueryExecutor,
): Promise<IOctolensMappingDiscoveryHealth | null> {
  const row: {
    activeMappings: number
    integrations: number
    lastDiscoveredAt: string | null
    checkedAt: string
  } = await qx.selectOne(
    `
      SELECT
        count(*)::int AS "activeMappings",
        count(DISTINCT m."integrationId")::int AS "integrations",
        max(m."lastSeenAt") AS "lastDiscoveredAt",
        now() AS "checkedAt"
      FROM integration."octolensKeywordMappings" m
      JOIN public.integrations i ON i.id = m."integrationId"
      WHERE m."deactivatedAt" IS NULL
        AND i."deletedAt" IS NULL
        AND ${OCTOLENS_SUBPROJECT_INTEGRATION}
    `,
    { platform: PlatformType.OCTOLENS },
  )
  if (row.lastDiscoveredAt === null) {
    return null
  }
  return {
    activeMappings: row.activeMappings,
    integrations: row.integrations,
    lastDiscoveredAt: new Date(row.lastDiscoveredAt),
    checkedAt: new Date(row.checkedAt),
  }
}

export interface IDeactivatedOctolensKeywordMapping {
  integrationId: string
  keywordId: number
  keyword: string
  segmentId: string
  segmentName: string
  segmentSlug: string
  deactivatedAt: Date
  integrationDeleted: boolean
  nowMappedToSegmentName: string | null
}

// Soft-deleted integrations stay in: discovery soft-deletes a subproject in the same run that
// deactivates its last mapping, and that is the deactivation most worth reporting.
export async function listOctolensKeywordMappingsDeactivatedBetween(
  qx: QueryExecutor,
  since: Date,
  until: Date,
): Promise<IDeactivatedOctolensKeywordMapping[]> {
  const rows: (Omit<IDeactivatedOctolensKeywordMapping, 'deactivatedAt'> & {
    deactivatedAt: string
  })[] = await qx.select(
    `
      SELECT
        m."integrationId",
        m."keywordId",
        m.keyword,
        m."segmentId",
        s.name AS "segmentName",
        s.slug AS "segmentSlug",
        m."deactivatedAt",
        i."deletedAt" IS NOT NULL AS "integrationDeleted",
        moved.name AS "nowMappedToSegmentName"
      FROM integration."octolensKeywordMappings" m
      JOIN public.integrations i ON i.id = m."integrationId"
      JOIN public.segments s ON s.id = m."segmentId"
      LEFT JOIN LATERAL (
        SELECT ms.name
        FROM integration."octolensKeywordMappings" a
        JOIN public.integrations ai ON ai.id = a."integrationId"
        JOIN public.segments ms ON ms.id = a."segmentId"
        WHERE a."keywordId" = m."keywordId"
          AND a."deactivatedAt" IS NULL
          AND ai."deletedAt" IS NULL
          AND ai.platform = $(platform)
          AND ai."segmentId" IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM integration.sync_units asu
            WHERE asu."integrationId" = ai.id AND asu.status = 'active'
          )
        ORDER BY ms.name
        LIMIT 1
      ) moved ON true
      WHERE m."deactivatedAt" > $(since)
        AND m."deactivatedAt" <= $(until)
        AND ${OCTOLENS_SUBPROJECT_INTEGRATION}
      ORDER BY m."deactivatedAt", s.name, m.keyword, m."keywordId"
    `,
    { platform: PlatformType.OCTOLENS, since, until },
  )
  return rows.map((row) => ({ ...row, deactivatedAt: new Date(row.deactivatedAt) }))
}
