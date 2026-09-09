import { PageData } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

type FakeOrganizationSuggestion = {
  organizationId: string
  displayName: string
  logo: string | null
  activityCount: number
}

export async function insertFakeOrganizationSuggestions(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<void> {
  if (organizationIds.length === 0) {
    return
  }

  const query = prepareBulkInsert(
    'fakeOrganizationSuggestions',
    ['organizationId', 'createdAt'],
    organizationIds.map((organizationId) => ({
      organizationId,
      createdAt: new Date(),
    })),
    '("organizationId") DO NOTHING',
  )

  await qx.result(query)
}

export async function deleteFakeOrganizationSuggestion(
  qx: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qx.result(
    `DELETE FROM "fakeOrganizationSuggestions" WHERE "organizationId" = $(organizationId)`,
    { organizationId },
  )
}

export async function findFakeOrganizationSuggestions(
  qx: QueryExecutor,
  segmentId: string,
  limit: number,
  offset: number,
): Promise<PageData<FakeOrganizationSuggestion>> {
  const params = { segmentId, limit, offset }

  const from = `
    FROM "fakeOrganizationSuggestions" fos
    JOIN "organizationSegmentsAgg" osa
      ON osa."organizationId" = fos."organizationId"
     AND osa."segmentId" = $(segmentId)
    JOIN organizations o
      ON o.id = fos."organizationId"
     AND o."deletedAt" IS NULL
  `

  const [rows, countRow] = await Promise.all([
    qx.select(
      `
      SELECT
        fos."organizationId",
        o."displayName",
        o.logo,
        osa."activityCount"
      ${from}
      ORDER BY osa."activityCount" DESC, fos."organizationId"
      LIMIT $(limit) OFFSET $(offset)
      `,
      params,
    ),
    qx.selectOne(`SELECT COUNT(*) ${from}`, params),
  ])

  return {
    rows,
    count: parseInt(countRow.count, 10),
    limit,
    offset,
  }
}
