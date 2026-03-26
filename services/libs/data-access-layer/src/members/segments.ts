import { DEFAULT_TENANT_ID } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'
import { SegmentData } from '@crowd/types'

import {
  IManualAffiliationData,
  IOrganizationMemberCount,
  IWorkExperienceData,
} from '../old/apps/data_sink_worker/repo/memberAffiliation.data'
import { QueryExecutor } from '../queryExecutor'
import { buildSegmentActivityTypes, isSegmentSubproject } from '../segments'
import { prepareBulkInsert } from '../utils'

import { BLACKLISTED_MEMBER_TITLES } from './base'
import { IMemberActivitySummary, IMemberSegmentAggregates } from './types'

const log = getServiceChildLogger('members/segments')

export async function findLastSyncDate(qx: QueryExecutor, memberId: string): Promise<Date | null> {
  const result = await qx.selectOneOrNone(
    `SELECT MAX("createdAt") AS "lastSyncDate" FROM "memberSegmentsAgg" WHERE "memberId" = $(memberId)`,
    { memberId },
  )
  return result?.lastSyncDate ? new Date(result.lastSyncDate) : null
}

export async function cleanupMemberAggregates(qx: QueryExecutor, memberId: string) {
  return qx.result(
    `
      DELETE FROM "memberSegmentsAgg"
      WHERE "memberId" = $(memberId)
    `,
    {
      memberId,
    },
  )
}

export async function insertMemberSegmentAggregates(
  qx: QueryExecutor,
  data: IMemberSegmentAggregates[],
) {
  try {
    return qx.result(
      prepareBulkInsert(
        'memberSegmentsAgg',
        [
          'memberId',
          'segmentId',
          'tenantId',

          'activityCount',
          'lastActive',
          'activityTypes',
          'activeOn',
          'averageSentiment',
        ],
        data.map((d) => {
          return {
            ...d,
            tenantId: DEFAULT_TENANT_ID,
          }
        }),
        `("memberId", "segmentId") DO UPDATE SET "activityCount" = EXCLUDED."activityCount",
                       "lastActive" = EXCLUDED."lastActive",
                       "activityTypes" = EXCLUDED."activityTypes",
                       "activeOn" = EXCLUDED."activeOn",
                       "averageSentiment" = EXCLUDED."averageSentiment"`,
      ),
    )
  } catch (e) {
    log.error(e, 'Error while inserting member segments!')
    throw e
  }
}

export async function fetchManyMemberSegments(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; segments: IMemberSegmentAggregates[] }[]> {
  return qx.select(
    `
      SELECT
        "memberId",
        JSONB_AGG(msa) AS segments
      FROM "memberSegmentsAgg" msa
      WHERE "memberId" = ANY($(memberIds)::UUID[])
      GROUP BY "memberId"
    `,
    {
      memberIds,
    },
  )
}

export async function fetchAbsoluteMemberAggregates(
  qx: QueryExecutor,
  memberId: string,
): Promise<IMemberActivitySummary> {
  return qx.selectOneOrNone(
    `
      SELECT SUM("activityCount") as "activityCount",
             MAX("lastActive") as "lastActive"
      FROM "memberSegmentsAgg"
      WHERE "memberId" = $(memberId);
    `,
    {
      memberId,
    },
  )
}

export async function includeMemberToSegments(
  qx: QueryExecutor,
  memberId: string,
  segmentIds: string[],
): Promise<void> {
  if (segmentIds.length === 0) {
    return
  }

  let bulkInsertMemberSegments = `INSERT INTO "memberSegments" ("memberId","segmentId", "tenantId", "createdAt") VALUES `
  const replacements = {
    memberId,
    tenantId: DEFAULT_TENANT_ID,
  }

  for (let idx = 0; idx < segmentIds.length; idx++) {
    bulkInsertMemberSegments += ` ($(memberId), $(segmentId${idx}), $(tenantId), now()) `

    replacements[`segmentId${idx}`] = segmentIds[idx]

    if (idx !== segmentIds.length - 1) {
      bulkInsertMemberSegments += `,`
    }
  }

  bulkInsertMemberSegments += ` ON CONFLICT ("memberId", "segmentId", "tenantId") DO NOTHING`

  await qx.result(bulkInsertMemberSegments, replacements)
}

export async function getMemberSegments(
  qx: QueryExecutor,
  memberId: string,
): Promise<SegmentData[]> {
  const query = `
        with member_segments as (
          select distinct on ("segmentId") "segmentId", "createdAt"
          from "memberSegments"
          where "memberId" = $(memberId)
        )
        select s.* 
        from segments s inner join member_segments ms on s.id = ms."segmentId"
        order by ms."createdAt" desc
    `

  let data = await qx.select(query, { memberId })

  data = data.map((s) => {
    return {
      ...s,
      activityTypes: null,
    }
  })

  for (const record of data) {
    if (isSegmentSubproject(record)) {
      record.activityTypes = buildSegmentActivityTypes(record)
    }
  }

  return data
}

// old memberAffiliation.repo -> findManualAffiliation
export async function findMemberManualAffiliation(
  qx: QueryExecutor,
  memberId: string,
  segmentId: string,
  timestamp: string,
): Promise<IManualAffiliationData | null> {
  const result = await qx.selectOneOrNone(
    `
      SELECT * FROM "memberSegmentAffiliations"
      WHERE "memberId" = $(memberId)
        AND "segmentId" = $(segmentId)
        AND (
          ("dateStart" <= $(timestamp) AND "dateEnd" >= $(timestamp))
          OR ("dateStart" <= $(timestamp) AND "dateEnd" IS NULL)
        )
      ORDER BY "dateStart" DESC, id
      LIMIT 1
    `,
    {
      memberId,
      segmentId,
      timestamp,
    },
  )
  return result
}

// old memberAffiliation.repo -> findWorkExperience
export async function findMemberWorkExperience(
  qx: QueryExecutor,
  memberId: string,
  timestamp: string,
): Promise<IWorkExperienceData[] | null> {
  const result = await qx.select(
    `
      SELECT
          mo.*,
          coalesce(ovr."isPrimaryWorkExperience", false) as "isPrimaryWorkExperience"
      FROM "memberOrganizations" mo
      LEFT JOIN "memberOrganizationAffiliationOverrides" ovr on ovr."memberOrganizationId" = mo."id"
      WHERE mo."memberId" = $(memberId)
        AND (
          (mo."dateStart" <= $(timestamp) AND mo."dateEnd" >= $(timestamp))
          OR (mo."dateStart" <= $(timestamp) AND mo."dateEnd" IS NULL)
        )
        AND mo."deletedAt" IS NULL
        AND coalesce(ovr."allowAffiliation", true) = true
      ORDER BY mo."dateStart" DESC, mo.id
    `,
    {
      memberId,
      timestamp,
    },
  )

  return filterOutBlacklistedTitles(result)
}

export async function findMemberCountEstimateOfOrganizations(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<IOrganizationMemberCount[] | null> {
  const result = await qx.select(
    `
      SELECT
        osa."organizationId",
        sum(osa."memberCount") AS "memberCount"
    FROM "organizationSegmentsAgg" osa
    WHERE osa."segmentId" IN (
        SELECT id
        FROM segments
        WHERE "grandparentId" is not null
            AND "parentId" is not null
    )
    and osa."organizationId" IN ($(organizationIds:csv))
    group by osa."organizationId"
    order by "memberCount" desc
    `,
    {
      organizationIds,
    },
  )

  return organizationIds
    .map((orgId) => {
      const org = result.find((r) => r.organizationId === orgId)
      return {
        organizationId: orgId,
        memberCount: org?.memberCount || 0,
      }
    })
    .sort((a, b) => b.memberCount - a.memberCount)
}

export async function findMostRecentUnknownDatedOrganizations(
  qx: QueryExecutor,
  memberId: string,
  timestamp: string,
): Promise<IWorkExperienceData[] | null> {
  const result = await qx.select(
    `
      SELECT
        mo.*,
        coalesce(ovr."isPrimaryWorkExperience", false) as "isPrimaryWorkExperience"
      FROM "memberOrganizations" mo
      LEFT JOIN "memberOrganizationAffiliationOverrides" ovr on ovr."memberOrganizationId" = mo.id
      WHERE mo."memberId" = $(memberId)
        AND mo."dateStart" IS NULL
        AND mo."dateEnd" IS NULL
        AND mo."createdAt" <= $(timestamp)
        AND mo."deletedAt" IS NULL
        AND coalesce(ovr."allowAffiliation", true) = true
      ORDER BY mo."createdAt" DESC, mo.id
    `,
    {
      memberId,
      timestamp,
    },
  )

  return filterOutBlacklistedTitles(result)
}

export async function findAllUnkownDatedOrganizations(
  qx: QueryExecutor,
  memberId: string,
): Promise<IWorkExperienceData[] | null> {
  const result = await qx.select(
    `
      SELECT
        mo.*,
        coalesce(ovr."isPrimaryWorkExperience", false) as "isPrimaryWorkExperience"
      FROM "memberOrganizations" mo
      LEFT JOIN "memberOrganizationAffiliationOverrides" ovr on ovr."memberOrganizationId" = mo.id
      WHERE mo."memberId" = $(memberId)
        AND mo."dateStart" IS NULL
        AND mo."dateEnd" IS NULL
        AND mo."deletedAt" IS NULL
        AND coalesce(ovr."allowAffiliation", true) = true
      ORDER BY mo."createdAt", mo.id
    `,
    {
      memberId,
    },
  )

  return filterOutBlacklistedTitles(result)
}

function filterOutBlacklistedTitles(experiences: IWorkExperienceData[]): IWorkExperienceData[] {
  return experiences.filter(
    (row) =>
      !row.title ||
      (row.title !== null &&
        row.title !== undefined &&
        !BLACKLISTED_MEMBER_TITLES.some((t) => row.title.toLowerCase().includes(t))),
  )
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
 * @param batchSize - Number of members to process at a time (default: 10000)
 * @param onProgress - Optional callback for progress updates (batchNumber, totalProcessed)
 * @returns Total number of aggregate rows inserted/updated
 */
export async function calculateMemberAggregatesForSegment(
  readQx: QueryExecutor,
  writeQx: QueryExecutor,
  targetSegmentId: string,
  sourceSegmentIds: string[],
  batchSize = 10000,
  onProgress?: (batchNumber: number, totalProcessed: number) => void,
): Promise<number> {
  let lastMemberId: string | null = null
  let totalProcessed = 0
  let batchNumber = 0

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const aggregates: IMemberSegmentAggregates[] = await readQx.select(
      `
      WITH scalar_aggs AS (
        SELECT
          "memberId",
          SUM("activityCount") AS "activityCount",
          MAX("lastActive") AS "lastActive",
          AVG("averageSentiment") AS "averageSentiment"
        FROM "memberSegmentsAgg"
        WHERE "segmentId" = ANY($(sourceSegmentIds)::UUID[])
          AND ($(lastMemberId)::UUID IS NULL OR "memberId" > $(lastMemberId)::UUID)
        GROUP BY "memberId"
        ORDER BY "memberId"
        LIMIT $(batchSize)
      ),
      array_aggs AS (
        SELECT
          "memberId",
          COALESCE(ARRAY_AGG(DISTINCT unnested_type) FILTER (WHERE unnested_type IS NOT NULL), ARRAY[]::TEXT[]) AS "activityTypes",
          COALESCE(ARRAY_AGG(DISTINCT unnested_platform) FILTER (WHERE unnested_platform IS NOT NULL), ARRAY[]::TEXT[]) AS "activeOn"
        FROM "memberSegmentsAgg"
        LEFT JOIN LATERAL unnest("activityTypes") AS unnested_type ON TRUE
        LEFT JOIN LATERAL unnest("activeOn") AS unnested_platform ON TRUE
        WHERE "segmentId" = ANY($(sourceSegmentIds)::UUID[])
          AND "memberId" IN (SELECT "memberId" FROM scalar_aggs)
        GROUP BY "memberId"
      )
      SELECT
        s."memberId",
        $(targetSegmentId)::UUID AS "segmentId",
        s."activityCount",
        s."lastActive",
        a."activityTypes",
        a."activeOn",
        s."averageSentiment"
      FROM scalar_aggs s
      JOIN array_aggs a ON s."memberId" = a."memberId"
      ORDER BY s."memberId"
      `,
      { targetSegmentId, sourceSegmentIds, batchSize, lastMemberId },
    )

    if (aggregates.length === 0) {
      break
    }

    // Insert this batch
    await insertMemberSegmentAggregates(writeQx, aggregates)

    totalProcessed += aggregates.length
    batchNumber++

    if (onProgress) {
      onProgress(batchNumber, totalProcessed)
    }

    // Update cursor to the last memberId processed
    lastMemberId = aggregates[aggregates.length - 1].memberId

    // If we got fewer results than the batch size, we're done
    if (aggregates.length < batchSize) {
      break
    }
  }

  return totalProcessed
}

/**
 * TEST ONLY - Calculate leaf segment (subproject) aggregates for members from activityRelations
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
export async function calculateAllMemberLeafAggregates(
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
    const aggregates: IMemberSegmentAggregates[] = await readQx.select(
      `
      SELECT
        ar."memberId",
        ar."segmentId",
        COUNT(ar."activityId") AS "activityCount",
        MAX(ar.timestamp) AS "lastActive",
        COALESCE(ARRAY_AGG(DISTINCT ar.platform) FILTER (WHERE ar.platform IS NOT NULL), ARRAY[]::TEXT[]) AS "activeOn",
        COALESCE(ARRAY_AGG(DISTINCT (ar.platform || ':' || ar.type)) FILTER (WHERE ar.platform IS NOT NULL AND ar.type IS NOT NULL), ARRAY[]::TEXT[]) AS "activityTypes",
        AVG(ar."sentimentScore") AS "averageSentiment"
      FROM "activityRelations" ar
      INNER JOIN segments s ON ar."segmentId" = s.id
      WHERE s."parentId" IS NOT NULL AND s."grandparentId" IS NOT NULL
      GROUP BY ar."memberId", ar."segmentId"
      ORDER BY ar."memberId", ar."segmentId"
      LIMIT $(batchSize) OFFSET $(offset)
      `,
      { batchSize, offset },
    )

    if (aggregates.length === 0) {
      break
    }

    // Insert this batch
    await insertMemberSegmentAggregates(
      writeQx,
      aggregates.map((a) => ({
        memberId: a.memberId,
        segmentId: a.segmentId,
        activityCount: Number(a.activityCount),
        lastActive: a.lastActive || '1970-01-01',
        activityTypes: a.activityTypes || [],
        activeOn: a.activeOn || [],
        averageSentiment: a.averageSentiment,
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
