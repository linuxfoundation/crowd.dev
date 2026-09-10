import { LlmQueryType, PageData } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

type FakeOrganizationSuggestion = {
  organizationId: string
  displayName: string
  logo: string | null
  activityCount: number
}

export async function fetchFakeOrganizationAnalysisCandidates(
  qx: QueryExecutor,
  limit: number,
  afterOrganizationId?: string,
): Promise<string[]> {
  const rows = await qx.select(
    `
      SELECT DISTINCT o.id AS "organizationId"
      FROM "organizationIdentities" oi
      JOIN organizations o ON o.id = oi."organizationId"
      WHERE oi.verified = true
        AND oi.platform = 'email'
        AND oi.type = 'primary-domain'
        AND o."deletedAt" IS NULL
        AND o."createdAt" < now() - interval '1 day'
        ${afterOrganizationId ? `AND o.id > $(afterOrganizationId)` : ''}
        AND EXISTS (
          SELECT 1
          FROM "memberOrganizations" mo
          WHERE mo."organizationId" = o.id
            AND mo.source = 'email-domain'
            AND mo."deletedAt" IS NULL
        )
        AND (
          SELECT COUNT(DISTINCT mo."memberId")
          FROM "memberOrganizations" mo
          JOIN members m ON m.id = mo."memberId" AND m."deletedAt" IS NULL
          WHERE mo."organizationId" = o.id
            AND mo."deletedAt" IS NULL
        ) = 1
        AND NOT EXISTS (
          SELECT 1
          FROM "llmPromptHistory" h
          WHERE h.type = $(type)
            AND h."entityId" = o.id::text
        )
      ORDER BY o.id
      LIMIT $(limit)
    `,
    {
      limit,
      afterOrganizationId,
      type: LlmQueryType.FAKE_ORGANIZATION_ANALYSIS,
    },
  )

  return rows.map((r) => r.organizationId)
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
