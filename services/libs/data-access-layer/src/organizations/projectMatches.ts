import { PageData } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export type OrganizationProjectMatchStatus = 'pending' | 'approved' | 'rejected'

export type OrganizationProjectMatchSource = 'segment-created' | 'organization-created' | 'backfill'

export interface OrganizationProjectMatch {
  id: string
  status: OrganizationProjectMatchStatus
  detectedBy: OrganizationProjectMatchSource
  createdAt: string
  organizationId: string
  displayName: string
  logo: string | null
  activityCount: number
  isAffiliationBlocked: boolean
  matchedOrganizationName: string
  segmentId: string
  segmentName: string
  segmentSlug: string
  segmentType: string
  matchedSegmentName: string
}

export interface OrganizationProjectPair {
  organizationId: string
  segmentId: string
}

// A project often has a group, a project and a sub-project with one PCC id: the match is stored
// on the topmost of those levels, so one organization gets one row per project.
export async function insertOrganizationProjectMatches(
  qx: QueryExecutor,
  {
    pairs,
    detectedBy,
  }: {
    pairs: OrganizationProjectPair[]
    detectedBy: OrganizationProjectMatchSource
  },
): Promise<number> {
  if (pairs.length === 0) {
    return 0
  }

  return qx.result(
    `
    INSERT INTO "organizationProjectMatches"
      ("organizationId", "segmentId", "detectedBy", "organizationName", "segmentName")
    SELECT DISTINCT ON (o.id, project.id)
      o.id,
      project.id,
      $(detectedBy),
      o."displayName",
      s.name
    FROM unnest($(organizationIds)::uuid[], $(segmentIds)::uuid[])
      AS pair("organizationId", "segmentId")
    JOIN organizations o ON o.id = pair."organizationId"
    JOIN segments s ON s.id = pair."segmentId"
    LEFT JOIN segments p ON p.id = s."parentId"
    LEFT JOIN segments gp ON gp.id = s."grandparentId"
    CROSS JOIN LATERAL (
      SELECT COALESCE(
        p."isLF"
          AND (
            NULLIF(p."sourceId", '') = NULLIF(s."sourceId", '')
            OR trim(lower(p.name)) = trim(lower(s.name))
          ),
        FALSE
      ) AS parent
    ) parent_level
    CROSS JOIN LATERAL (
      SELECT parent_level.parent AND COALESCE(
        gp."isLF"
          AND (
            NULLIF(gp."sourceId", '') = NULLIF(s."sourceId", '')
            OR trim(lower(gp.name)) = trim(lower(s.name))
          ),
        FALSE
      ) AS grandparent
    ) grandparent_level
    CROSS JOIN LATERAL (
      SELECT CASE
        WHEN grandparent_level.grandparent THEN gp.id
        WHEN parent_level.parent THEN p.id
        ELSE s.id
      END AS id
    ) project
    WHERE s."isLF"
      AND o."deletedAt" IS NULL
      AND NOT o."isAffiliationBlocked"
    ORDER BY o.id, project.id, s.name, s.id
    ON CONFLICT ("organizationId", "segmentId") DO NOTHING
    `,
    {
      organizationIds: pairs.map((pair) => pair.organizationId),
      segmentIds: pairs.map((pair) => pair.segmentId),
      detectedBy,
    },
  )
}

export async function findOrganizationProjectMatches(
  qx: QueryExecutor,
  {
    status = 'pending',
    limit,
    offset,
  }: { status?: OrganizationProjectMatchStatus; limit: number; offset: number },
): Promise<PageData<OrganizationProjectMatch> & { hasMore: boolean }> {
  const params = { status, limit, offset }

  const matches = `
    FROM "organizationProjectMatches" opm
    JOIN organizations o
      ON o.id = opm."organizationId"
     AND o."deletedAt" IS NULL
  `

  const [rows, countRow] = await Promise.all([
    qx.select(
      `
      SELECT
        opm.id,
        opm.status,
        opm."detectedBy",
        opm."createdAt",
        opm."organizationId",
        o."displayName",
        o.logo,
        COALESCE(gac.total_count_estimate, 0) AS "activityCount",
        o."isAffiliationBlocked",
        opm."organizationName" AS "matchedOrganizationName",
        opm."segmentId",
        s.name AS "segmentName",
        s.slug AS "segmentSlug",
        s.type AS "segmentType",
        opm."segmentName" AS "matchedSegmentName"
      ${matches}
      JOIN segments s ON s.id = opm."segmentId"
      LEFT JOIN "organizationsGlobalActivityCount" gac
        ON gac."organizationId" = opm."organizationId"
      WHERE opm.status = $(status)
      ORDER BY "activityCount" DESC, opm.id
      LIMIT $(limit) OFFSET $(offset)
      `,
      params,
    ),
    qx.selectOne(`SELECT COUNT(*) ${matches} WHERE opm.status = $(status)`, params),
  ])

  const count = parseInt(countRow.count, 10)

  return {
    rows,
    count,
    limit,
    offset,
    hasMore: offset + rows.length < count,
  }
}

export async function rejectOrganizationProjectMatch(
  qx: QueryExecutor,
  id: string,
  decidedById: string | null,
): Promise<boolean> {
  const updated = await qx.result(
    `
    UPDATE "organizationProjectMatches"
    SET status = 'rejected',
        "decidedById" = $(decidedById),
        "decidedAt" = NOW(),
        "updatedAt" = NOW()
    WHERE id = $(id)
      AND status = 'pending'
    `,
    { id, decidedById },
  )
  return updated > 0
}

export async function approvePendingOrganizationProjectMatches(
  qx: QueryExecutor,
  organizationId: string,
  decidedById: string | null,
): Promise<number> {
  return qx.result(
    `
    UPDATE "organizationProjectMatches"
    SET status = 'approved',
        "decidedById" = $(decidedById),
        "decidedAt" = NOW(),
        "updatedAt" = NOW()
    WHERE "organizationId" = $(organizationId)
      AND status = 'pending'
    `,
    { organizationId, decidedById },
  )
}
