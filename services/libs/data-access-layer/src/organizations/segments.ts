import { DEFAULT_TENANT_ID } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

import { IDbOrganizationAggregateData } from './types'

const log = getServiceChildLogger('organizations/segments')

export interface IOrganizationSegments {
  organizationId: string
  segments: string[]
}

export async function cleanupForOganization(qx: QueryExecutor, organizationId: string) {
  return qx.result(
    `
      DELETE FROM "organizationSegmentsAgg"
      WHERE "organizationId" = $(organizationId)
    `,
    {
      organizationId,
    },
  )
}

export async function insertOrganizationSegments(
  qx: QueryExecutor,
  data: IDbOrganizationAggregateData[],
) {
  try {
    return qx.result(
      prepareBulkInsert(
        'organizationSegmentsAgg',
        [
          'organizationId',
          'segmentId',
          'tenantId',

          'joinedAt',
          'lastActive',
          'activeOn',
          'activityCount',
          'memberCount',
          'avgContributorEngagement',
        ],
        data.map((d) => {
          return {
            tenantId: DEFAULT_TENANT_ID,
            ...d,
          }
        }),
        `("organizationId", "segmentId") DO UPDATE SET "joinedAt" = EXCLUDED."joinedAt",
                       "lastActive" = EXCLUDED."lastActive",
                       "activeOn" = EXCLUDED."activeOn",
                       "activityCount" = EXCLUDED."activityCount",
                       "memberCount" = EXCLUDED."memberCount",
                       "avgContributorEngagement" = EXCLUDED."avgContributorEngagement"`,
      ),
    )
  } catch (e) {
    log.error(e, 'Error while inserting organization segments!')
    throw e
  }
}

export async function fetchManyOrgSegments(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<IOrganizationSegments[]> {
  const result = await Promise.all(
    organizationIds.map((organizationId) =>
      qx.selectOneOrNone(
        `
          SELECT
            "organizationId",
            ARRAY_AGG("segmentId") AS segments
          FROM "organizationSegmentsAgg"
          WHERE "organizationId" = $(organizationId)
          GROUP BY "organizationId"
        `,
        {
          organizationId,
        },
      ),
    ),
  )

  return result.filter((row) => !!row)
}

export async function fetchTotalActivityCount(
  qx: QueryExecutor,
  organizationId: string,
): Promise<number> {
  const res: { activityCount: number } = await qx.selectOneOrNone(
    `
      SELECT SUM("activityCount") as "activityCount"
      FROM "organizationSegmentsAgg"
      WHERE "organizationId" = $(organizationId);
    `,
    {
      organizationId,
    },
  )

  return res?.activityCount || 0
}

/**
 * Calculate and insert aggregates for a target segment by rolling up data from source segments.
 * Processes in batches to avoid memory issues with large datasets.
 * Uses keyset pagination (cursor-based) for efficient pagination on large datasets.
 *
 * @param readQx - Query executor for reading data
 * @param writeQx - Query executor for writing data
 * @param targetSegmentId - The segment ID to calculate aggregates for (project or project group)
 * @param sourceSegmentIds - The source segment IDs to roll up from (subprojects for projects, or subprojects for project groups)
 * @param batchSize - Number of organizations to process at a time (default: 10000)
 * @param onProgress - Optional callback for progress updates (batchNumber, totalProcessed)
 * @returns Total number of aggregate rows inserted/updated
 */
export async function calculateOrganizationAggregatesForSegment(
  readQx: QueryExecutor,
  writeQx: QueryExecutor,
  targetSegmentId: string,
  sourceSegmentIds: string[],
  batchSize = 10000,
  onProgress?: (batchNumber: number, totalProcessed: number) => void,
): Promise<number> {
  let lastOrganizationId: string | null = null
  let totalProcessed = 0
  let batchNumber = 0

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const aggregates: IDbOrganizationAggregateData[] = await readQx.select(
      `
      WITH scalar_aggs AS (
        SELECT
          "organizationId",
          SUM("activityCount") AS "activityCount",
          COALESCE(MIN("joinedAt") FILTER (WHERE "joinedAt" <> '1970-01-01'), '1970-01-01'::TIMESTAMP WITH TIME ZONE) AS "joinedAt",
          MAX("lastActive") AS "lastActive",
          COALESCE(ROUND(AVG("avgContributorEngagement")), 0) AS "avgContributorEngagement"
        FROM "organizationSegmentsAgg"
        WHERE "segmentId" = ANY($(sourceSegmentIds)::UUID[])
          AND ($(lastOrganizationId)::UUID IS NULL OR "organizationId" > $(lastOrganizationId)::UUID)
        GROUP BY "organizationId"
        ORDER BY "organizationId"
        LIMIT $(batchSize)
      ),
      -- Count distinct members from activityRelations to avoid double-counting
      -- members who appear in multiple leaf segments
      member_counts AS (
        SELECT
          ar."organizationId",
          COUNT(DISTINCT ar."memberId") AS "memberCount"
        FROM "activityRelations" ar
        WHERE ar."segmentId" = ANY($(sourceSegmentIds)::UUID[])
          AND ar."organizationId" IS NOT NULL
          AND ar."organizationId" IN (SELECT "organizationId" FROM scalar_aggs)
        GROUP BY ar."organizationId"
      ),
      array_aggs AS (
        SELECT
          "organizationId",
          COALESCE(ARRAY_AGG(DISTINCT unnested_platform) FILTER (WHERE unnested_platform IS NOT NULL), ARRAY[]::TEXT[]) AS "activeOn"
        FROM "organizationSegmentsAgg"
        LEFT JOIN LATERAL unnest("activeOn") AS unnested_platform ON TRUE
        WHERE "segmentId" = ANY($(sourceSegmentIds)::UUID[])
          AND "organizationId" IN (SELECT "organizationId" FROM scalar_aggs)
        GROUP BY "organizationId"
      )
      SELECT
        s."organizationId",
        $(targetSegmentId)::UUID AS "segmentId",
        s."activityCount",
        COALESCE(mc."memberCount", 0) AS "memberCount",
        s."joinedAt",
        s."lastActive",
        a."activeOn",
        s."avgContributorEngagement"
      FROM scalar_aggs s
      JOIN array_aggs a ON s."organizationId" = a."organizationId"
      LEFT JOIN member_counts mc ON s."organizationId" = mc."organizationId"
      ORDER BY s."organizationId"
      `,
      { targetSegmentId, sourceSegmentIds, batchSize, lastOrganizationId },
    )

    if (aggregates.length === 0) {
      break
    }

    // Insert this batch
    await insertOrganizationSegments(writeQx, aggregates)

    totalProcessed += aggregates.length
    batchNumber++

    if (onProgress) {
      onProgress(batchNumber, totalProcessed)
    }

    // Update cursor to the last organizationId processed
    lastOrganizationId = aggregates[aggregates.length - 1].organizationId

    // If we got fewer results than the batch size, we're done
    if (aggregates.length < batchSize) {
      break
    }
  }

  return totalProcessed
}

/**
 * TEST ONLY - Calculate leaf segment (subproject) aggregates for organizations from activityRelations
 * in batches to avoid memory issues.
 *
 * In production, leaf segment aggregates come from Tinybird via Kafka Connect.
 * This function is for local testing when Tinybird data is not available.
 *
 * @param readQx - Query executor for reading data
 * @param writeQx - Query executor for writing data
 * @param batchSize - Number of rows to process at a time (default: 10000)
 * @param onProgress - Optional callback for progress updates (batchNumber, totalProcessed)
 * @returns Total number of aggregate rows inserted/updated
 */
export async function calculateAllOrganizationLeafAggregates(
  readQx: QueryExecutor,
  writeQx: QueryExecutor,
  batchSize = 10000,
  onProgress?: (batchNumber: number, totalProcessed: number) => void,
): Promise<number> {
  let offset = 0
  let totalProcessed = 0
  let batchNumber = 0

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const aggregates: IDbOrganizationAggregateData[] = await readQx.select(
      `
      SELECT
        ar."organizationId",
        ar."segmentId",
        COUNT(ar."activityId") AS "activityCount",
        COUNT(DISTINCT ar."memberId") AS "memberCount",
        MIN(ar.timestamp) AS "joinedAt",
        MAX(ar.timestamp) AS "lastActive",
        COALESCE(ARRAY_AGG(DISTINCT ar.platform) FILTER (WHERE ar.platform IS NOT NULL), ARRAY[]::TEXT[]) AS "activeOn",
        COALESCE(ROUND(AVG(ar.score)), 0) AS "avgContributorEngagement"
      FROM "activityRelations" ar
      INNER JOIN segments s ON ar."segmentId" = s.id
      WHERE ar."organizationId" IS NOT NULL
        AND s."parentId" IS NOT NULL
        AND s."grandparentId" IS NOT NULL
      GROUP BY ar."organizationId", ar."segmentId"
      ORDER BY ar."organizationId", ar."segmentId"
      LIMIT $(batchSize) OFFSET $(offset)
      `,
      { batchSize, offset },
    )

    if (aggregates.length === 0) {
      break
    }

    // Insert this batch
    await insertOrganizationSegments(
      writeQx,
      aggregates.map((a) => ({
        organizationId: a.organizationId,
        segmentId: a.segmentId,
        activityCount: Number(a.activityCount),
        memberCount: Number(a.memberCount),
        joinedAt: a.joinedAt || '1970-01-01',
        lastActive: a.lastActive || '1970-01-01',
        activeOn: a.activeOn || [],
        avgContributorEngagement: Number(a.avgContributorEngagement) || 0,
      })),
    )

    totalProcessed += aggregates.length
    batchNumber++

    if (onProgress) {
      onProgress(batchNumber, totalProcessed)
    }

    // If we got fewer results than the batch size, we're done
    if (aggregates.length < batchSize) {
      break
    }

    offset += batchSize
  }

  return totalProcessed
}
