import { v4 as uuid } from 'uuid'

import { IMemberAffiliation } from '@crowd/types'

import { IManualAffiliationData } from '../old/apps/data_sink_worker/repo/memberAffiliation.data'
import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

export async function deleteMemberAffiliations(qx: QueryExecutor, memberId: string) {
  await qx.result(
    `
      DELETE FROM "memberSegmentAffiliations"
      WHERE "memberId" = $(memberId)
    `,
    { memberId },
  )
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
    `,
    { fromMemberId, toMemberId, affiliationIds },
  )
}
