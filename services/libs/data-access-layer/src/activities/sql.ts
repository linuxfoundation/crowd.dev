/* eslint-disable @typescript-eslint/no-explicit-any */
import max from 'lodash.max'
import min from 'lodash.min'
import moment from 'moment'

import {
  ActivityRelations,
  ActivityTimeseriesDatapoint,
  Counter,
  DbConnOrTx,
  TinybirdClient,
} from '@crowd/database'
import { ActivityDisplayService } from '@crowd/integrations'
import {
  ActivityTypeSettings,
  IActivityBySentimentMoodResult,
  IActivityByTypeAndPlatformResult,
  ITimeseriesDatapoint,
  PageData,
} from '@crowd/types'

import { getLatestMemberActivityRelations } from '../activityRelations'
import { MemberField, queryMembers } from '../members/base'
import {
  IActivityRelationCreateOrUpdateData,
  IActivityRelationUpdateById,
} from '../old/apps/data_sink_worker/repo/activity.data'
import { findOrgsByIds } from '../organizations'
import { QueryExecutor } from '../queryExecutor'

import { buildActivitiesParams } from './tinybirdAdapter'
import {
  IActivitySentiment,
  IQueryActivitiesParameters,
  IQueryActivityResult,
  IQueryGroupedActivitiesParameters,
} from './types'

export async function getActivitiesById(
  conn: DbConnOrTx,
  ids: string[],
  segmentIds: string[],
  qx: QueryExecutor,
  activityTypeSettings: ActivityTypeSettings,
): Promise<IQueryActivityResult[]> {
  if (ids.length === 0) {
    return []
  }

  const data = await queryActivities(
    {
      filter: { and: [{ id: { in: ids } }] },
      limit: ids.length,
      segmentIds,
    },
    qx,
    activityTypeSettings,
  )

  return data.rows
}

export const ACTIVITY_ALL_COLUMNS: ActivityColumn[] = [
  'id',
  'type',
  'timestamp',
  'platform',
  'score',
  'sourceId',
  'createdAt',
  'updatedAt',
  'deletedAt',
  'memberId',
  'parentId',
  'createdById',
  'updatedById',
  'sourceParentId',
  'conversationId',
  'attributes',
  'title',
  'body',
  'channel',
  'url',
  'username',
  'objectMemberId',
  'objectMemberUsername',
  'segmentId',
  'organizationId',
  'sentimentLabel',
  'sentimentScore',
  'sentimentScoreMixed',
  'sentimentScoreNeutral',
  'sentimentScoreNegative',
  'sentimentScorePositive',
  'member_isBot',
  'member_isTeamMember',
  'gitIsMainBranch',
  'gitIsIndirectFork',
  'gitLines',
  'gitInsertions',
  'gitDeletions',
  'gitIsMerge',
]

export async function setMemberDataToActivities(
  conn: DbConnOrTx,
  memberId: string,
  data: { isBot: boolean; isTeamMember: boolean },
): Promise<void> {
  await conn.none(
    `
      update activities set
        "member_isBot" = $(isBot),
        "member_isTeamMember" = $(isTeamMember)
      where "memberId" = $(memberId);
    `,
    {
      memberId,
      ...data,
    },
  )
}

export async function addActivityToConversation(
  conn: DbConnOrTx,
  id: string,
  conversationId: string,
): Promise<void> {
  await conn.none(
    `
    UPDATE activities SET "conversationId" = $(conversationId) WHERE id = $(id);
    `,
    {
      id,
      conversationId,
    },
  )
}

export type ActivityColumn =
  | 'id'
  | 'type'
  | 'timestamp'
  | 'platform'
  | 'score'
  | 'sourceId'
  | 'createdAt'
  | 'updatedAt'
  | 'deletedAt'
  | 'memberId'
  | 'parentId'
  | 'createdById'
  | 'updatedById'
  | 'sourceParentId'
  | 'conversationId'
  | 'attributes'
  | 'title'
  | 'body'
  | 'channel'
  | 'url'
  | 'username'
  | 'objectMemberId'
  | 'objectMemberUsername'
  | 'segmentId'
  | 'organizationId'
  | 'sentimentLabel'
  | 'sentimentScore'
  | 'sentimentScoreMixed'
  | 'sentimentScoreNeutral'
  | 'sentimentScoreNegative'
  | 'sentimentScorePositive'
  | 'member_isBot'
  | 'member_isTeamMember'
  | 'gitIsMainBranch'
  | 'gitIsIndirectFork'
  | 'gitInsertions'
  | 'gitDeletions'
  | 'gitLines'
  | 'gitIsMerge'

export const DEFAULT_COLUMNS_TO_SELECT: ActivityColumn[] = [
  'id',
  'attributes',
  'body',
  'channel',
  'conversationId',
  'createdAt',
  'createdById',
  'memberId',
  'username',
  'objectMemberId',
  'objectMemberUsername',
  'organizationId',
  'parentId',
  'platform',
  'score',
  'segmentId',
  'sentimentLabel',
  'sentimentScore',
  'sentimentScoreMixed',
  'sentimentScoreNeutral',
  'sentimentScoreNegative',
  'sentimentScorePositive',
  'sourceId',
  'sourceParentId',
  'timestamp',
  'title',
  'type',
  'updatedAt',
  'updatedById',
  'url',
]

export const ALL_COLUMNS_TO_SELECT: ActivityColumn[] = DEFAULT_COLUMNS_TO_SELECT.concat([
  'member_isBot',
  'member_isTeamMember',
  'gitIsMainBranch',
  'gitIsIndirectFork',
  'gitLines',
  'gitInsertions',
  'gitDeletions',
  'gitIsMerge',
])

function extractUniqueIds(activities: Array<{ organizationId?: string; memberId?: string }>): {
  orgIds: string[]
  memberIds: string[]
} {
  const { org, mem } = activities.reduce(
    (
      acc: { org: Set<string>; mem: Set<string> },
      a: { organizationId?: string; memberId?: string },
    ) => {
      const orgId = a.organizationId?.trim?.() ?? a.organizationId
      const memId = a.memberId?.trim?.() ?? a.memberId
      if (orgId) acc.org.add(orgId)
      if (memId) acc.mem.add(memId)
      return acc
    },
    { org: new Set<string>(), mem: new Set<string>() },
  )

  return {
    orgIds: [...org],
    memberIds: [...mem],
  }
}

export async function queryActivities(
  arg: IQueryActivitiesParameters,
  qx: QueryExecutor,
  activityTypeSettings?: ActivityTypeSettings,
): Promise<PageData<IQueryActivityResult | any>> {
  if (arg.segmentIds === undefined || arg.segmentIds.length === 0) {
    throw new Error('segmentIds are required to query activities!')
  }

  const tb = new TinybirdClient()

  const tbParams = buildActivitiesParams(arg)

  const tbActivities = await tb.pipeSql<{ data: ActivityRelations[] }>(
    'activities_relations_filtered',
    tbParams,
  )

  const { orgIds, memberIds } = extractUniqueIds(tbActivities.data)

  const [membersInfo, orgsInfo] = await Promise.all([
    memberIds.length
      ? queryMembers(qx, {
          filter: { id: { in: memberIds } },
          fields: [MemberField.ATTRIBUTES, MemberField.ID, MemberField.DISPLAY_NAME],
        })
      : Promise.resolve([]),
    orgIds.length ? findOrgsByIds(qx, orgIds) : Promise.resolve([]),
  ])

  const membersMap = Object.fromEntries(membersInfo.map((item) => [item.id, item]))

  const organizationsMap = Object.fromEntries(orgsInfo.map((item) => [item.id, item]))

  const enrichedActivities = tbActivities.data.map((activity) => {
    const org = activity.organizationId ? organizationsMap[activity.organizationId] : undefined
    const mem = activity.memberId ? membersMap[activity.memberId] : undefined

    const display = activityTypeSettings
      ? ActivityDisplayService.getDisplayOptions(activity, activityTypeSettings)
      : {}

    return {
      ...activity,
      ...(org && {
        organization: {
          id: org.id,
          displayName: org.displayName,
        },
      }),
      ...(mem && {
        member: {
          id: mem.id,
          displayName: mem.displayName,
          attributes: {
            avatarUrl: mem.attributes?.avatarUrl,
            isBot: mem.attributes?.isBot,
          },
        },
      }),
      display,
    }
  })

  let countTb = 0
  if (!arg.noCount) {
    const countResp = await tb.pipe<{ data: { count: number }[] }>(
      'activities_relations_filtered',
      {
        ...tbParams,
        countOnly: 1,
      },
    )
    countTb = Number(countResp?.data?.[0]?.count ?? 0)
  }

  return {
    count: Number(countTb),
    rows: enrichedActivities,
    limit: arg.limit,
    offset: arg.offset,
  }
}

export async function queryActivitiesCounter(
  arg: IQueryActivitiesParameters & { indirectFork?: number },
  tbClient: TinybirdClient,
): Promise<{ data: Counter }> {
  const payload = {
    ...buildActivitiesParams(arg),
    ...(arg.indirectFork && { indirectFork: arg.indirectFork }),
    countOnly: 1,
  }

  return tbClient.pipe('activities_relations_filtered', payload)
}

export function mapActivityRowToResult(a: any, columns: string[]): any {
  const sentiment: IActivitySentiment | null =
    a.sentimentLabel &&
    a.sentimentScore &&
    a.sentimentScoreMixed &&
    a.sentimentScoreNeutral &&
    a.sentimentScoreNegative &&
    a.sentimentScorePositive
      ? {
          label: a.sentimentLabel,
          sentiment: a.sentimentScore,
          mixed: a.sentimentScoreMixed,
          neutral: a.sentimentScoreNeutral,
          negative: a.sentimentScoreNegative,
          positive: a.sentimentScorePositive,
        }
      : null

  const data: any = {}
  for (const column of columns) {
    if (column.startsWith('sentiment')) {
      continue
    }

    if (column === 'attributes') {
      data[column] = JSON.parse(a[column])
    } else {
      data[column] = a[column]
    }
  }

  if (sentiment) {
    data.sentiment = sentiment
  }

  return data
}

function fillMissingDays(
  data: { date: string; count: number }[],
  startDate: Date,
  endDate: Date,
): ITimeseriesDatapoint[] {
  const result: ITimeseriesDatapoint[] = []

  //handles both "2025-09-18 00:00:00" and "2025-09-18T00:00:00" formats
  const dataMap = new Map(
    data.map((d) => {
      // Extract only day (YYYY-MM-DD)
      const dateOnly = d.date.includes('T')
        ? d.date.split('T')[0] // ISO format: "2025-09-18T00:00:00"
        : d.date.split(' ')[0] // Space format: "2025-09-18 00:00:00"

      return [dateOnly, d.count]
    }),
  )

  const current = moment(startDate)
  const end = moment(endDate)

  while (current.isBefore(end, 'day')) {
    const dateKey = current.format('YYYY-MM-DD')
    const isoDate = current.format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z'

    result.push({
      date: isoDate,
      count: dataMap.get(dateKey) || 0, // Use existing data or 0 if missing
    })

    current.add(1, 'day')
  }

  return result
}

export async function activitiesTimeseries(
  arg: IQueryGroupedActivitiesParameters,
): Promise<ITimeseriesDatapoint[]> {
  const tb = new TinybirdClient()
  const timeseries = await tb.pipe<{ data: ActivityTimeseriesDatapoint[] }>(
    'activities_daily_counts',
    {
      after: arg.startDate,
      before: arg.endDate,
      platform: arg.platform,
      segmentIds: arg.segmentIds.join(','),
    },
  )
  if (arg.startDate && arg.endDate) {
    timeseries.data = fillMissingDays(timeseries.data, arg.startDate, arg.endDate)
  }

  return timeseries.data.map((row) => ({
    count: Number(row.count),
    date: row.date,
  }))
}

export async function activitiesBySentiment(
  qdbConn: DbConnOrTx,
  arg: IQueryGroupedActivitiesParameters,
): Promise<IActivityBySentimentMoodResult[]> {
  let query = `
    SELECT COUNT_DISTINCT(id) AS count, sentimentLabel
    FROM activities
    WHERE "deletedAt" IS NULL
    AND "sentimentLabel" IS NOT NULL
  `

  if (arg.segmentIds) {
    query += ' AND "segmentId" IN ($(segmentIds:csv))'
  }

  if (arg.platform) {
    query += ' AND "platform" = $(platform)'
  }

  if (arg.startDate && arg.endDate) {
    query += ' AND "timestamp" BETWEEN $(after) AND $(before)'
  }

  query += ` GROUP BY sentimentLabel;`

  const rows: IActivityBySentimentMoodResult[] = await qdbConn.query(query, {
    segmentIds: arg.segmentIds,
    platform: arg.platform,
    after: arg.startDate,
    before: arg.endDate,
  })

  rows.forEach((row) => {
    row.count = Number(row.count)
  })

  return rows
}

export async function activitiesByTypeAndPlatform(
  qdbConn: DbConnOrTx,
  arg: IQueryGroupedActivitiesParameters,
): Promise<IActivityByTypeAndPlatformResult[]> {
  let query = `
    SELECT COUNT_DISTINCT(id) AS count, platform, type
    FROM activities
    WHERE "deletedAt" IS NULL
  `

  if (arg.segmentIds) {
    query += ' AND "segmentId" IN ($(segmentIds:csv))'
  }

  if (arg.platform) {
    query += ' AND "platform" = $(platform)'
  }

  if (arg.startDate && arg.endDate) {
    query += ' AND "timestamp" BETWEEN $(after) AND $(before)'
  }

  query += ` GROUP BY platform, type ORDER BY count DESC;`

  const rows: IActivityByTypeAndPlatformResult[] = await qdbConn.query(query, {
    segmentIds: arg.segmentIds,
    platform: arg.platform,
    after: arg.startDate,
    before: arg.endDate,
  })

  rows.forEach((row) => {
    row.count = Number(row.count)
  })

  return rows
}

export async function getLastActivitiesForMembers(
  qx: QueryExecutor,
  memberIds: string[],
  activityTypeSettings?: ActivityTypeSettings,
  segmentIds?: string[],
): Promise<IQueryActivityResult[]> {
  const results = await getLatestMemberActivityRelations(qx, memberIds)

  if (results.length === 0) {
    return []
  }

  const activityIds = results.map((r) => r.activityId)
  const timestamps = results.map((r) => r.timestamp)

  const activities = await queryActivities(
    {
      filter: {
        and: [
          {
            id: {
              in: activityIds,
            },
          },
          {
            segmentId: {
              in: segmentIds || [],
            },
          },
          {
            timestamp:
              activityIds.length > 1
                ? {
                    gte: min(timestamps),
                    lte: max(timestamps),
                  }
                : { eq: timestamps[0] },
          },
        ],
      },
      segmentIds,
    },
    qx,
    activityTypeSettings,
  )

  return activities.rows
}

export async function createOrUpdateRelations(
  qe: QueryExecutor,
  relations: IActivityRelationCreateOrUpdateData[],
  skipChecks = false,
): Promise<void> {
  if (relations.length === 0) {
    return
  }

  const params: Record<string, unknown> = {}
  let index = 0

  const activityIds = new Set<string>()
  const valueList: string[] = []
  for (const data of relations) {
    if (data.username === undefined || data.username === null) {
      continue
    }

    if (data.platform === undefined || data.platform === null) {
      continue
    }

    if (data.segmentId === undefined || data.segmentId === null) {
      continue
    }

    if (!skipChecks) {
      // check objectMember exists
      if (data.objectMemberId !== undefined && data.objectMemberId !== null) {
        let objectMember = await qe.select(
          `
      SELECT id
      FROM members
      WHERE id = $(objectMemberId)
    `,
          {
            objectMemberId: data.objectMemberId,
          },
        )

        if (objectMember.length === 0) {
          if (data.objectMemberUsername && data.platform) {
            objectMember = await qe.select(
              `
          SELECT "memberId"
          FROM "memberIdentities"
          WHERE value = $(value) 
            and platform = $(platform) 
            and verified = true
            and "deletedAt" is null
          limit 1
        `,
              {
                value: data.objectMemberUsername,
                platform: data.platform,
              },
            )

            if (objectMember.length === 0) {
              data.objectMemberId = null
            } else {
              data.objectMemberId = objectMember[0].memberId
            }
          } else {
            data.objectMemberId = null
          }
        }
      }

      // check conversation exists
      if (data.conversationId !== undefined && data.conversationId !== null) {
        const conversation = await qe.select(
          `
      SELECT id
      FROM conversations
      WHERE id = $(conversationId)
    `,
          {
            conversationId: data.conversationId,
          },
        )

        if (conversation.length === 0) {
          data.conversationId = null
        }
      }

      // check segmentId exists
      const segment = await qe.select(
        `
      SELECT id
      FROM segments
      WHERE id = $(segmentId)
    `,
        {
          segmentId: data.segmentId,
        },
      )

      if (segment.length === 0) {
        // segment not found, skip adding this activity relation
        continue
      }

      // check member exists
      let member = await qe.select(
        `
      SELECT id
      FROM members
      WHERE id = $(memberId)
    `,
        {
          memberId: data.memberId,
        },
      )

      if (member.length === 0) {
        // find member using identity
        member = await qe.select(
          `
        SELECT "memberId"
        FROM "memberIdentities"
        WHERE value = $(value) 
          and platform = $(platform) 
          and verified = true
          and "deletedAt" is null
        limit 1
      `,
          {
            value: data.username,
            platform: data.platform,
          },
        )
        if (member.length === 0) {
          // member not found, skip adding this activity relation
          continue
        } else {
          data.memberId = member[0].memberId
        }
      }

      if (data.organizationId !== undefined && data.organizationId !== null) {
        const organization = await qe.select(
          `
        SELECT id
        FROM organizations
        WHERE id = $(organizationId)
      `,
          {
            organizationId: data.organizationId,
          },
        )

        if (organization.length === 0) {
          data.organizationId = null
        }
      }
    }

    if (activityIds.has(data.activityId)) {
      continue
    }

    activityIds.add(data.activityId)

    const activityIdParam = `activityId_${index++}`
    const memberIdParam = `memberId_${index++}`
    const objectMemberIdParam = `objectMemberId_${index++}`
    const organizationIdParam = `organizationId_${index++}`
    const conversationIdParam = `conversationId_${index++}`
    const parentIdParam = `parentId_${index++}`
    const segmentIdParam = `segmentId_${index++}`
    const platformParam = `platform_${index++}`
    const usernameParam = `username_${index++}`
    const objectMemberUsernameParam = `objectMemberUsername_${index++}`
    const sourceIdParam = `sourceId_${index++}`
    const sourceParentIdParam = `sourceParentId_${index++}`
    const typeParam = `type_${index++}`
    const timestampParam = `timestamp_${index++}`
    const channelParam = `channel_${index++}`
    const sentimentScoreParam = `sentimentScore_${index++}`
    const gitInsertionsParam = `gitInsertions_${index++}`
    const gitDeletionsParam = `gitDeletions_${index++}`
    const scoreParam = `score_${index++}`
    const isContributionParam = `isContribution_${index++}`
    const pullRequestReviewStateParam = `pullRequestReviewState_${index++}`

    params[activityIdParam] = data.activityId
    params[memberIdParam] = data.memberId
    params[objectMemberIdParam] = data.objectMemberId ?? null
    params[organizationIdParam] = data.organizationId ?? null
    params[conversationIdParam] = data.conversationId ?? null
    params[parentIdParam] = data.parentId ?? null
    params[segmentIdParam] = data.segmentId
    params[platformParam] = data.platform
    params[usernameParam] = data.username
    params[objectMemberUsernameParam] = data.objectMemberUsername ?? null
    params[sourceIdParam] = data.sourceId ?? null
    params[sourceParentIdParam] = data.sourceParentId ?? null
    params[typeParam] = data.type ?? null
    params[timestampParam] = data.timestamp ?? null
    params[channelParam] = data.channel ?? null
    params[sentimentScoreParam] = data.sentimentScore ?? null
    params[gitInsertionsParam] = data.gitInsertions ?? null
    params[gitDeletionsParam] = data.gitDeletions ?? null
    params[scoreParam] = data.score ?? null
    params[isContributionParam] = false
    params[pullRequestReviewStateParam] = data.pullRequestReviewState ?? null

    valueList.push(
      `
        (
          $(${activityIdParam}), 
          $(${memberIdParam}), 
          $(${objectMemberIdParam}), 
          $(${organizationIdParam}), 
          $(${conversationIdParam}), 
          $(${parentIdParam}), 
          $(${segmentIdParam}), 
          $(${platformParam}), 
          $(${usernameParam}), 
          $(${objectMemberUsernameParam}), 
          $(${sourceIdParam}), 
          $(${sourceParentIdParam}),
          $(${typeParam}), 
          $(${timestampParam}), 
          $(${channelParam}), 
          $(${sentimentScoreParam}), 
          $(${gitInsertionsParam}), 
          $(${gitDeletionsParam}), 
          $(${scoreParam}),
          $(${isContributionParam}), 
          $(${pullRequestReviewStateParam}), 
          now(), 
          now()
        )`,
    )
  }

  await qe.result(
    `
    INSERT INTO "activityRelations" (
            "activityId", 
            "memberId", 
            "objectMemberId", 
            "organizationId",
            "conversationId",
            "parentId",
            "segmentId",
            "platform",
            "username",
            "objectMemberUsername",
            "sourceId",
            "sourceParentId",
            "type",
            "timestamp",
            "channel",
            "sentimentScore",
            "gitInsertions",
            "gitDeletions",
            "score",
            "isContribution",
            "pullRequestReviewState",
            "createdAt", 
            "updatedAt")
    VALUES ${valueList.join(',')}

    ON CONFLICT ("timestamp", "platform", "type", "sourceId", "channel", "segmentId")
    DO UPDATE 
    SET 
        "updatedAt" = EXCLUDED."updatedAt",
        "memberId" = EXCLUDED."memberId",
        "objectMemberId" = EXCLUDED."objectMemberId",
        "organizationId" = EXCLUDED."organizationId",
        "platform" = EXCLUDED."platform",
        "username" = EXCLUDED."username",
        "objectMemberUsername" = EXCLUDED."objectMemberUsername",
        "sourceId" = EXCLUDED."sourceId",
        "sourceParentId" = EXCLUDED."sourceParentId",
        "type" = EXCLUDED."type",
        "timestamp" = EXCLUDED."timestamp",
        "channel" = EXCLUDED."channel",
        "sentimentScore" = EXCLUDED."sentimentScore",
        "gitInsertions" = EXCLUDED."gitInsertions",
        "gitDeletions" = EXCLUDED."gitDeletions",
        "score" = EXCLUDED."score",
        "isContribution" = EXCLUDED."isContribution",
        "pullRequestReviewState" = EXCLUDED."pullRequestReviewState";
    `,
    params,
  )
}

export async function updateActivityRelationsById(
  qe: QueryExecutor,
  data: IActivityRelationUpdateById,
): Promise<void> {
  const fields: string[] = []

  for (const [key, value] of Object.entries(data)) {
    if (value !== undefined && key !== 'activityId') {
      fields.push(`"${key}" = $(${key})`)
    }
  }

  if (fields.length === 0) return

  const query = `UPDATE "activityRelations" SET ${fields.join(', ')}, "updatedAt" = now() WHERE "activityId" = $(activityId)`

  await qe.result(query, data)
}

export interface IActivityRelationsCreateData {
  id: string
  memberId: string
  timestamp: string
  createdAt: string
  objectMemberId?: string
  organizationId?: string
  conversationId?: string
  parentId?: string
  segmentId: string
  platform: string
  username: string
  objectMemberUsername?: string
  sourceId: string
  sourceParentId?: string
  type: string
  channel: string
  sentimentScore: number
  gitInsertions: number
  gitDeletions: number
  score: number
  pullRequestReviewState?: string
}
