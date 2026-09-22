import { DEFAULT_TENANT_ID, getEnv } from '@crowd/common'
import { DbStore } from '@crowd/database'
import { IMember, IQueryTimeseriesParams, ITimeseriesDatapoint } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { IQueryNumberOfNewMembers } from './types'

const s3Url = `https://${
  process.env['CROWD_S3_MICROSERVICES_ASSETS_BUCKET']
}-${getEnv()}.s3.eu-central-1.amazonaws.com`

const tenantId = DEFAULT_TENANT_ID

export async function getMemberById(db: DbStore, id: string): Promise<IMember> {
  const query = `
    SELECT
      "id"
      "attributes", "displayName",
      "score",
      "enrichedBy",
      "joinedAt", "createdAt"
    FROM members
    WHERE "id" = $(id)
    AND "deletedAt" IS NULL;
  `

  const rows: IMember[] = await db.connection().query(query, {
    id,
  })

  rows.map((row) => {
    if (!row.avatarUrl) {
      row.avatarUrl = `${s3Url}/email/member-placeholder.png`
    }

    return row
  })

  return rows[0]
}

export async function getNumberOfNewMembers(
  db: DbStore,
  arg: IQueryNumberOfNewMembers,
): Promise<number> {
  let query = 'SELECT DISTINCT COUNT(m.id) FROM members m'

  if (arg.segmentIds) {
    query += ' INNER JOIN "memberSegmentsAgg" msa on msa."memberId" = m.id'
  }

  query += ` WHERE m."tenantId" = $(tenantId)
    AND m."createdAt" BETWEEN $(after) AND $(before)
    AND (COALESCE((((m.attributes -> 'isBot'::text) -> 'default'::text))::boolean, false)) IS FALSE
    AND (COALESCE((((m.attributes -> 'isTeamMember'::text) -> 'default'::text))::boolean, false)) IS FALSE
    AND (COALESCE((((m.attributes -> 'isOrganization'::text) -> 'default'::text))::boolean, false)) IS FALSE
    AND m."deletedAt" IS NULL`

  if (arg.segmentIds) {
    query += ' AND msa."segmentId" IN ($(segmentIds:csv))'
  }

  if (arg.platform) {
    query += ' AND $(platform) = ANY(msa."activeOn")'
  }

  query += ';'

  const rows: { count: number }[] = await db.connection().query(query, {
    tenantId,
    segmentIds: arg.segmentIds,
    after: arg.after,
    before: arg.before,
    platform: arg.platform,
  })

  return Number(rows[0].count || 0)
}

export interface IActiveMembersTimeseriesResult {
  date: string
  count: number
}

export async function getTimeseriesOfNewMembers(
  qx: QueryExecutor,
  params: IQueryTimeseriesParams,
): Promise<ITimeseriesDatapoint[]> {
  const query = `
    SELECT
      COUNT(DISTINCT m.id) AS count,
      TO_CHAR(m."joinedAt", 'YYYY-MM-DD') AS "date"
    FROM members AS m
    ${params.segmentIds ? 'INNER JOIN "memberSegmentsAgg" msa on msa."memberId" = m.id' : ''} -- if segments
    WHERE m."tenantId" = $(tenantId)
      AND m."joinedAt" >= $(startDate)
      AND m."joinedAt" < $(endDate)
      AND (COALESCE((((m.attributes -> 'isBot'::text) -> 'default'::text))::boolean, false)) IS FALSE
      AND (COALESCE((((m.attributes -> 'isTeamMember'::text) -> 'default'::text))::boolean, false)) IS FALSE
      AND (COALESCE((((m.attributes -> 'isOrganization'::text) -> 'default'::text))::boolean, false)) IS FALSE
      AND m."deletedAt" IS NULL
      ${params.segmentIds ? 'AND msa."segmentId" IN ($(segmentIds:csv))' : ''}
      ${params.platform ? 'AND $(platform) = ANY(msa."activeOn")' : ''}
    GROUP BY 2
    ORDER BY 2
  `

  return qx.select(query, { ...params, tenantId })
}

export async function getTimeseriesOfActiveMembers(
  qx: QueryExecutor,
  params: IQueryTimeseriesParams,
): Promise<ITimeseriesDatapoint[]> {
  const query = `
    SELECT
      COUNT(DISTINCT "memberId") AS count,      
      DATE_TRUNC('day', timestamp) AS "date"
    FROM "activityRelations"
    WHERE "memberId" IS NOT NULL
      ${params.segmentIds ? 'AND "segmentId" IN ($(segmentIds:csv))' : ''}
      AND timestamp >= $(startDate)
      AND timestamp < $(endDate)
      ${params.platform ? 'AND "platform" = $(platform)' : ''}
    GROUP BY 2
    ORDER BY 2
  `

  return qx.select(query, { ...params, tenantId })
}
