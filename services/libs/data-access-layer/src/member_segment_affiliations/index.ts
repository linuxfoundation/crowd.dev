import { v4 as uuid } from 'uuid'

import { IMemberAffiliation } from '@crowd/types'

import { IManualAffiliationData } from '../old/apps/data_sink_worker/repo/memberAffiliation.data'
import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

type MemberSegmentAffiliationDeleteFilter = {
  ids?: string[]
  memberId?: string
  organizationId?: string
  segmentId?: string
  mode?: 'soft' | 'hard'
}

export async function deleteMemberSegmentAffiliations(
  qx: QueryExecutor,
  {
    ids,
    memberId,
    organizationId,
    segmentId,
    mode = 'soft',
  }: MemberSegmentAffiliationDeleteFilter,
) {
  const where: string[] = []
  const params: Record<string, unknown> = {}

  if (memberId) {
    where.push(`"memberId" = $(memberId)`)
    params.memberId = memberId
  }

  if (organizationId) {
    where.push(`"organizationId" = $(organizationId)`)
    params.organizationId = organizationId
  }

  if (segmentId) {
    where.push(`"segmentId" = $(segmentId)`)
    params.segmentId = segmentId
  }

  if (ids?.length) {
    where.push(`"id" IN ($(ids:csv))`)
    params.ids = ids
  }

  if (where.length === 0) {
    throw new Error('At least one filter must be provided')
  }

  const whereClause = where.join(' AND ')

  const query =
    mode === 'hard'
      ? `
          DELETE FROM "memberSegmentAffiliations"
          WHERE ${whereClause}
        `
      : `
          UPDATE "memberSegmentAffiliations"
          SET "deletedAt" = NOW()
          WHERE ${whereClause}
            AND "deletedAt" IS NULL
        `

  return qx.result(query, params)
}

export async function findMemberAffiliations(
  qx: QueryExecutor,
  memberId: string,
): Promise<IManualAffiliationData[]> {
  return qx.select(
    `
      SELECT *
      FROM "memberSegmentAffiliations"
      WHERE "memberId" = $(memberId)
        AND "deletedAt" IS NULL
    `,
    { memberId },
  )
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function insertMemberAffiliations(qx: QueryExecutor, memberId: string, data: any[]) {
  return qx.result(
    prepareBulkInsert(
      'memberSegmentAffiliations',
      ['id', 'memberId', 'segmentId', 'organizationId', 'dateStart', 'dateEnd'],
      data.map((item) => ({
        id: uuid(),
        memberId,
        segmentId: item.segmentId,
        organizationId: item.organizationId,
        dateStart: item.dateStart || null,
        dateEnd: item.dateEnd || null,
      })),
    ),
  )
}

export async function fetchMemberAffiliations(
  qx: QueryExecutor,
  memberId: string,
): Promise<IMemberAffiliation[]> {
  return qx.select(
    `
        SELECT
          id,
          "dateStart",
          "dateEnd",
          "organizationId",
          "segmentId"
        FROM "memberSegmentAffiliations"
        WHERE "memberId" = $(memberId)
          AND "deletedAt" IS NULL
      `,
    {
      memberId,
    },
  )
}

export async function moveSelectedAffiliationsBetweenMembers(
  qx: QueryExecutor,
  fromMemberId: string,
  toMemberId: string,
  affiliationIds: string[],
): Promise<void> {
  if (affiliationIds.length === 0) return

  await qx.result(
    `
    UPDATE "memberSegmentAffiliations"
    SET "memberId" = $(toMemberId)
    WHERE "memberId" = $(fromMemberId)
      AND "id" IN ($(affiliationIds:csv))
      AND "deletedAt" IS NULL
    `,
    { fromMemberId, toMemberId, affiliationIds },
  )
}
