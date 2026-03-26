import { RawQueryParser } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'
import { IEnrichableMemberIdentityActivityAggregate, PageData } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

import {
  IActivityRelationColumn,
  IDbActivityRelation,
  IQueryActivityRelationsParameters,
} from './types'

const ALL_ACTIVITY_RELATION_COLUMNS: IActivityRelationColumn[] = [
  'activityId',
  'memberId',
  'objectMemberId',
  'organizationId',
  'conversationId',
  'parentId',
  'segmentId',
  'platform',
  'username',
  'objectMemberUsername',
  'sourceId',
  'sourceParentId',
  'type',
  'timestamp',
  'channel',
  'sentimentScore',
  'gitInsertions',
  'gitDeletions',
  'score',
  'pullRequestReviewState',
]

const logger = getServiceChildLogger('activityRelations')

export async function queryActivityRelations(
  qx: QueryExecutor,
  arg: IQueryActivityRelationsParameters,
  columns: IActivityRelationColumn[] = ALL_ACTIVITY_RELATION_COLUMNS,
): Promise<PageData<IDbActivityRelation>> {
  // Set defaults
  arg.filter = arg.filter || {}
  arg.orderBy =
    arg.orderBy && arg.orderBy.length > 0 ? arg.orderBy.filter((o) => o.trim().length > 0) : []
  arg.orderBy = arg.orderBy.length > 0 ? arg.orderBy : ['timestamp_DESC']
  arg.offset = arg.offset || 0
  arg.limit = arg.limit || 20
  arg.countOnly = arg.countOnly || false

  // Clean up empty conversationId arrays
  if (arg.filter.and) {
    for (const f of arg.filter.and) {
      if (f.conversationId && f.conversationId.in && f.conversationId.in.length === 0) {
        delete f.conversationId
      }
    }
  }

  // Parse orderBy entries
  const parsedOrderBys = arg.orderBy.map((orderByPart) => {
    const [column, dir] = orderByPart.split('_')
    const direction = dir?.toLowerCase()

    if (!columns.includes(column as IActivityRelationColumn)) {
      throw new Error(`Cannot order by column '${column}' that is not selected`)
    }

    if (!['asc', 'desc'].includes(direction)) {
      throw new Error(`Invalid sort direction: ${direction} for column: ${column}`)
    }

    return { column, direction }
  })

  const orderByString = parsedOrderBys.map((o) => `ar."${o.column}" ${o.direction}`).join(', ')

  const params: Record<string, unknown> = {
    segmentIds: arg.segmentIds,
  }

  const ACTIVITY_RELATIONS_QUERY_FILTER_COLUMN_MAP = new Map(
    ALL_ACTIVITY_RELATION_COLUMNS.map((col) => [col, `ar."${col}"`]),
  )

  let whereClause = RawQueryParser.parseFilters(
    arg.filter,
    ACTIVITY_RELATIONS_QUERY_FILTER_COLUMN_MAP,
    [],
    params,
    { pgPromiseFormat: true },
  )

  if (whereClause.trim().length === 0) {
    whereClause = '1=1'
  }

  if (arg.segmentIds && arg.segmentIds.length > 0) {
    whereClause += ` and ar."segmentId" in ($(segmentIds:csv))`
  } else {
    logger.warn('No segmentIds filter provided, querying all segments!')
  }

  let baseQuery = `
    from "activityRelations" ar
    where ${whereClause}
  `

  if (arg.groupBy) {
    baseQuery += ` group by ar."${arg.groupBy}"`
  }

  const countQuery = `
    select count(*) as count ${baseQuery}
  `

  if (arg.countOnly) {
    const countResults = await qx.select(countQuery, params)

    return {
      rows: [],
      count: Number(countResults[0]?.count || 0),
      limit: arg.limit,
      offset: arg.offset,
    }
  }

  const columnString = columns.map((c) => `ar."${c}"`).join(', ')

  let query = `
    select ${columnString}
    ${baseQuery}
    order by ${orderByString}
  `

  if (!arg.noLimit || arg.limit > 0) {
    query += ` limit $(limit) offset $(offset)`

    params.limit = arg.limit
    params.offset = arg.offset
  }

  // Execute both queries in parallel
  const [results, countResults] = await Promise.all([
    qx.select(query, params),
    arg.noCount ? Promise.resolve([{ count: 0 }]) : qx.select(countQuery, params),
  ])

  return {
    rows: results,
    count: Number(countResults[0]?.count || 0),
    limit: arg.limit,
    offset: arg.offset,
  }
}

export async function moveActivityRelationsToAnotherMember(
  qe: QueryExecutor,
  fromId: string,
  toId: string,
  batchSize = 5000,
) {
  let memberRowsUpdated

  do {
    const rowCount = await qe.result(
      `
          UPDATE "activityRelations"
          SET
            "memberId" = $(toId),
            "updatedAt" = now()
          WHERE "activityId" in (
            select "activityId" from "activityRelations"
            where "memberId" = $(fromId)
            limit $(batchSize)
          )
          returning "activityId"
        `,
      {
        toId,
        fromId,
        batchSize,
      },
    )

    memberRowsUpdated = rowCount
  } while (memberRowsUpdated === batchSize)

  let objectMemberRowsUpdated

  do {
    const rowCount = await qe.result(
      `
          UPDATE "activityRelations"
          SET
            "objectMemberId" = $(toId),
            "updatedAt" = now()
          WHERE "activityId" in (
            select "activityId" from "activityRelations"
            where "objectMemberId" = $(fromId)
            limit $(batchSize)
          )
          returning "activityId"
        `,
      {
        toId,
        fromId,
        batchSize,
      },
    )

    objectMemberRowsUpdated = rowCount
  } while (objectMemberRowsUpdated === batchSize)
}

export async function moveActivityRelationsWithIdentityToAnotherMember(
  qe: QueryExecutor,
  fromId: string,
  toId: string,
  username: string,
  platform: string,
  batchSize = 5000,
) {
  let memberRowsUpdated

  do {
    const rowCount = await qe.result(
      `
          UPDATE "activityRelations"
          SET
            "memberId" = $(toId),
            "updatedAt" = now()
          WHERE "activityId" in (
            select "activityId" from "activityRelations"
            where 
              "memberId" = $(fromId) and
              "username" = $(username) and
              "platform" = $(platform)
            limit $(batchSize)
          )
          returning "activityId"
        `,
      {
        toId,
        fromId,
        username,
        platform,
        batchSize,
      },
    )

    memberRowsUpdated = rowCount
  } while (memberRowsUpdated === batchSize)

  let objectMemberRowsUpdated

  do {
    const rowCount = await qe.result(
      `
          UPDATE "activityRelations"
          SET
            "objectMemberId" = $(toId),
            "updatedAt" = now()
          WHERE "activityId" in (
            select "activityId" from "activityRelations"
            where 
              "objectMemberId" = $(fromId) and
              "objectMemberUsername" = $(username) and
              "platform" = $(platform)
            limit $(batchSize)
          )
          returning "activityId"
        `,
      {
        toId,
        fromId,
        username,
        platform,
        batchSize,
      },
    )

    objectMemberRowsUpdated = rowCount
  } while (objectMemberRowsUpdated === batchSize)
}

export async function moveActivityRelationsToAnotherOrganization(
  qe: QueryExecutor,
  fromId: string,
  toId: string,
  batchSize = 5000,
) {
  let rowsUpdated

  do {
    const rowCount = await qe.result(
      `
          UPDATE "activityRelations"
          SET
            "organizationId" = $(toId),
            "updatedAt" = now()
          WHERE "activityId" in (
            select "activityId" from "activityRelations"
            where "organizationId" = $(fromId)
            limit $(batchSize)
          )
          returning "activityId"
        `,
      {
        toId,
        fromId,
        batchSize,
      },
    )

    rowsUpdated = rowCount
  } while (rowsUpdated === batchSize)
}

export async function findMemberIdentityWithTheMostActivityInPlatform(
  qx: QueryExecutor,
  memberId: string,
  platform: string,
): Promise<IEnrichableMemberIdentityActivityAggregate> {
  return await qx.selectOneOrNone(
    `
    SELECT count(a.id) AS "activityCount", a.platform, a.username
      FROM "activityRelations" a
      WHERE a."memberId" = $(memberId)
        AND a.platform = $(platform)
      GROUP BY a.platform, a.username
      ORDER BY "activityCount" DESC
    LIMIT 1;
    `,
    { memberId, platform },
  )
}

export async function filterMembersWithActivityRelations(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<string[]> {
  const results = await qx.select(
    `select distinct "memberId" from "activityRelations" where "memberId" in ($(memberIds:csv))`,
    { memberIds },
  )

  return results.map((r) => r.memberId)
}

export async function getLatestMemberActivityRelations(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ activityId: string; timestamp: string }[]> {
  return qx.select(
    `
    select distinct on ("memberId") "activityId", timestamp
    from "activityRelations"
    where "memberId" in ($(memberIds:csv))
    order by "memberId", "timestamp" desc;
  `,
    { memberIds },
  )
}

export interface IActivityRelationDuplicateGroup {
  activityIds: string[]
  timestamp: string
  platform: string
  type: string
  sourceId: string
  channel: string
  segmentId: string
}

export async function fetchActivityRelationsDuplicateGroups(
  qx: QueryExecutor,
  limit: number,
  cursor?: Omit<IActivityRelationDuplicateGroup, 'activityIds'>,
): Promise<IActivityRelationDuplicateGroup[]> {
  return qx.select(
    `
    WITH grouped_activity_relations AS (
      SELECT
        "timestamp", "platform", "type", "sourceId", "channel", "segmentId",
        array_agg("activityId" ORDER BY "updatedAt" DESC) AS "activityIds"
      FROM "activityRelations"
      WHERE
        "timestamp" IS NOT NULL AND "platform" IS NOT NULL AND 
        "type" IS NOT NULL AND "sourceId" IS NOT NULL AND 
        "channel" IS NOT NULL AND "segmentId" IS NOT NULL
        ${
          cursor
            ? `AND ("timestamp", "platform", "type", "sourceId", "channel", "segmentId") >
                 ($(timestamp), $(platform), $(type), $(sourceId), $(channel), $(segmentId))`
            : ''
        }
      GROUP BY
        "timestamp", "platform", "type", "sourceId", "channel", "segmentId"
      HAVING COUNT(*) > 1
      ORDER BY "timestamp", "platform", "type", "sourceId", "channel", "segmentId"
    )
    SELECT * FROM grouped_activity_relations LIMIT $(limit);
    `,
    { limit, ...(cursor || {}) },
  )
}

export async function deleteActivityRelationsById(
  qx: QueryExecutor,
  activityIds: string[],
): Promise<void> {
  await qx.result(
    `
    DELETE FROM "activityRelations" WHERE "activityId" IN ($(activityIds:csv));
    `,
    { activityIds },
  )
}
