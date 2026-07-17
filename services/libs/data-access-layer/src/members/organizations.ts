import { generateUUIDv1 } from '@crowd/common'
import {
  IMemberOrganization,
  IMemberOrganizationAffiliationOverride,
  IMemberRoleWithOrganization,
  MemberOrganizationDbInsert,
  MemberOrganizationDbRow,
  MemberOrganizationUpdate,
  OrganizationSource,
} from '@crowd/types'

import {
  changeMemberOrganizationAffiliationOverrides,
  findMemberAffiliationOverrides,
  findOrganizationAffiliationOverrides,
} from '../member-organization-affiliation'
import { deleteMemberSegmentAffiliations } from '../member_segment_affiliations'
import { EntityType } from '../old/apps/script_executor_worker/types'
import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

import { EmailDomainMemberOrganizationActivityDate } from './types'

/* eslint-disable @typescript-eslint/no-explicit-any */

const toIsoString = (v: Date | string): string =>
  v instanceof Date ? v.toISOString() : new Date(v).toISOString()

export async function fetchMemberOrganizations(
  qx: QueryExecutor,
  memberId: string,
): Promise<IMemberOrganization[]> {
  return qx.select(
    `
      SELECT "id", "organizationId", "dateStart", "dateEnd", "title", "memberId", "source"
      FROM "memberOrganizations"
      WHERE "memberId" = $(memberId)
      AND "deletedAt" IS NULL
      ORDER BY
          CASE
              WHEN "dateEnd" IS NULL AND "dateStart" IS NOT NULL THEN 1
              WHEN "dateEnd" IS NOT NULL AND "dateStart" IS NOT NULL THEN 2
              WHEN "dateEnd" IS NULL AND "dateStart" IS NULL THEN 3
              ELSE 4
              END ASC,
          "dateEnd" DESC,
          "dateStart" DESC
    `,
    {
      memberId,
    },
  )
}

export async function fetchMemberOrganizationById(
  qx: QueryExecutor,
  id: string,
): Promise<IMemberOrganization | undefined> {
  return qx.selectOneOrNone(
    `SELECT * FROM "memberOrganizations" WHERE "id" = $(id) AND "deletedAt" IS NULL`,
    { id },
  )
}

/**
 * Fetches member organizations for a source, optionally including soft-deleted rows.
 */
export async function fetchMemberOrganizationsBySource(
  qx: QueryExecutor,
  memberId: string,
  source: OrganizationSource,
  { withDeleted = false }: { withDeleted?: boolean } = {},
): Promise<IMemberOrganization[]> {
  const deletedClause = withDeleted ? '' : 'AND "deletedAt" IS NULL'

  return qx.select(
    `
      SELECT
        "id",
        "organizationId",
        "dateStart",
        "dateEnd",
        "title",
        "memberId",
        "source",
        "deletedAt"
      FROM "memberOrganizations"
      WHERE "memberId" = $(memberId)
        AND "source" = $(source)
        ${deletedClause}
    `,
    { memberId, source },
  )
}

export async function fetchEmailDomainMemberOrganizationsWithoutDates(
  qx: QueryExecutor,
  limit: number,
  afterMemberId?: string,
): Promise<string[]> {
  const rows = await qx.select(
    `
      SELECT DISTINCT "memberId"
      FROM "memberOrganizations"
      WHERE "source" = 'email-domain'
        AND "dateStart" IS NULL
        AND "dateEnd" IS NULL
        AND "deletedAt" IS NULL
        ${afterMemberId ? `AND "memberId" > $(afterMemberId)` : ''}
      ORDER BY "memberId"
      LIMIT $(limit)
    `,
    { limit, afterMemberId },
  )

  return rows.map((r) => r.memberId)
}

export async function fetchEmailDomainMemberOrganizationActivityDates(
  qx: QueryExecutor,
  memberId: string,
): Promise<EmailDomainMemberOrganizationActivityDate[]> {
  return qx.select(
    `
      WITH email_domain_member_orgs AS (
        SELECT DISTINCT
          mo."memberId",
          mo."organizationId",
          lower(oi.value) AS domain
        FROM "memberOrganizations" mo
        INNER JOIN "organizationIdentities" oi
          ON oi."organizationId" = mo."organizationId"
          AND oi.type = 'primary-domain'
          AND oi.verified = true
        WHERE mo."memberId" = $(memberId)
          AND mo."source" = 'email-domain'
          AND mo."deletedAt" IS NULL
      )
      SELECT DISTINCT
        edmo."memberId",
        edmo."organizationId",
        ar."timestamp"::date::text AS date
      FROM email_domain_member_orgs edmo
      INNER JOIN "memberIdentities" mi
        ON mi."memberId" = edmo."memberId"
        AND mi.verified = true
        AND mi.type = 'email'
        AND mi."deletedAt" IS NULL
        AND lower(split_part(mi.value, '@', 2)) = edmo.domain
      INNER JOIN "activityRelations" ar
        ON ar."memberId" = mi."memberId"
        AND ar.platform = mi.platform
        AND lower(ar.username) = lower(mi.value)
        AND ar."timestamp" IS NOT NULL
      ORDER BY edmo."memberId", edmo."organizationId", date
    `,
    { memberId },
  )
}

export async function fetchOrganizationMemberIds(
  qx: QueryExecutor,
  organizationId: string,
  limit: number,
  afterMemberId?: string,
): Promise<string[]> {
  const result = await qx.select(
    `
      SELECT DISTINCT "memberId"
      FROM "memberOrganizations"
      WHERE "organizationId" = $(organizationId)
        AND "deletedAt" IS NULL
        ${afterMemberId ? `AND "memberId" > $(afterMemberId)` : ''}
      ORDER BY "memberId"
      LIMIT $(limit);
    `,
    {
      organizationId,
      limit,
      afterMemberId,
    },
  )

  return result.map((r) => r.memberId)
}

export async function fetchManyMemberOrgs(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; organizations: IMemberOrganization[] }[]> {
  return qx.select(
    `
      SELECT
        mo."memberId",
        JSONB_AGG(
          TO_JSONB(mo) || JSONB_BUILD_OBJECT(
            'affiliationOverride', 
            CASE WHEN moao."isPrimaryWorkExperience" IS NOT NULL 
                 THEN JSONB_BUILD_OBJECT('isPrimaryWorkExperience', moao."isPrimaryWorkExperience")
                 ELSE NULL 
            END
          ) ORDER BY mo."createdAt"
        ) AS "organizations"
      FROM "memberOrganizations" mo
      LEFT JOIN "memberOrganizationAffiliationOverrides" moao 
        ON moao."memberOrganizationId" = mo.id
      WHERE mo."memberId" IN ($(memberIds:csv))
        AND mo."deletedAt" IS NULL
      GROUP BY mo."memberId"
    `,
    {
      memberIds,
    },
  )
}

export async function fetchManyMemberOrgsWithOrgData(
  qx: QueryExecutor,
  memberIds: string[],
  { withDomains = false }: { withDomains?: boolean } = {},
): Promise<Map<string, IMemberRoleWithOrganization[]>> {
  const domainSelect = withDomains
    ? `,
      COALESCE(oid.domains, '{}'::text[]) AS "organizationDomains"`
    : ''

  const domainJoin = withDomains
    ? `
      LEFT JOIN (
        SELECT
          oi."organizationId",
          array_agg(DISTINCT lower(oi.value) ORDER BY lower(oi.value)) AS domains
        FROM "organizationIdentities" oi
        WHERE oi.type = 'primary-domain'
          AND oi.verified = true
          AND oi."organizationId" IN (
            SELECT DISTINCT mo2."organizationId"
            FROM "memberOrganizations" mo2
            WHERE mo2."memberId" IN ($(memberIds:csv))
              AND mo2."deletedAt" IS NULL
          )
        GROUP BY oi."organizationId"
      ) oid ON oid."organizationId" = mo."organizationId"`
    : ''

  const sql = `
    SELECT
      mo.*,
      o."displayName" AS "organizationName",
      o.logo AS "organizationLogo"
      ${domainSelect}
    FROM "memberOrganizations" mo
    JOIN organizations o ON o.id = mo."organizationId"
    ${domainJoin}
    WHERE mo."memberId" IN ($(memberIds:csv))
      AND mo."deletedAt" IS NULL;
  `

  const memberRoles = (await qx.select(sql, {
    memberIds,
  })) as IMemberRoleWithOrganization[]

  const result = new Map<string, IMemberRoleWithOrganization[]>()

  for (const memberId of memberIds) {
    result.set(memberId, [])
  }

  for (const role of memberRoles) {
    const roles = result.get(role.memberId)
    if (roles) {
      roles.push(role)
    }
  }

  return result
}

export async function fetchManyOrganizationAffiliationPolicies(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<Map<string, boolean>> {
  if (organizationIds.length === 0) return new Map()

  const results = await qx.select(
    `SELECT id, "isAffiliationBlocked"
     FROM organizations
     WHERE id IN ($(organizationIds:csv))`,
    { organizationIds },
  )

  return new Map(
    results.map((r: { id: string; isAffiliationBlocked: boolean }) => [
      r.id,
      r.isAffiliationBlocked ?? false,
    ]),
  )
}

export async function insertMemberOrganizations(
  qx: QueryExecutor,
  organizations: MemberOrganizationDbInsert[],
  failOnConflict: boolean,
  returnRows: true,
): Promise<MemberOrganizationDbRow[]>
export async function insertMemberOrganizations(
  qx: QueryExecutor,
  organizations: MemberOrganizationDbInsert[],
  failOnConflict?: boolean,
  returnRows?: false,
): Promise<number>
export async function insertMemberOrganizations(
  qx: QueryExecutor,
  organizations: MemberOrganizationDbInsert[],
  failOnConflict = false,
  returnRows = false,
): Promise<MemberOrganizationDbRow[] | number> {
  const ts = new Date()

  if (organizations.length === 0) {
    return returnRows ? [] : 0
  }

  const query = prepareBulkInsert(
    'memberOrganizations',
    [
      'id',
      'memberId',
      'organizationId',
      'dateStart',
      'dateEnd',
      'title',
      'source',
      'verified',
      'verifiedBy',
      'createdAt',
      'updatedAt',
    ],
    organizations.map((o) => ({
      ...o,
      id: o.id ?? generateUUIDv1(),
      // NOT NULL, no DB default
      createdAt: ts,
      updatedAt: ts,
      // NOT NULL DEFAULT false — must set while column is in INSERT list
      verified: o.verified ?? false,
    })),
    failOnConflict ? undefined : 'DO NOTHING',
    returnRows,
  )

  if (returnRows) {
    return qx.select(query)
  }

  return qx.result(query)
}

export async function createMemberOrganization(
  qx: QueryExecutor,
  memberId: string,
  data: Partial<IMemberOrganization>,
): Promise<string | undefined> {
  const rows = await insertMemberOrganizations(
    qx,
    [
      {
        memberId,
        organizationId: data.organizationId,
        dateStart: data.dateStart != null ? toIsoString(data.dateStart) : null,
        dateEnd: data.dateEnd != null ? toIsoString(data.dateEnd) : null,
        title: data.title,
        source: data.source,
        verified: data.verified,
        verifiedBy: data.verifiedBy,
      },
    ],
    false,
    true,
  )

  return rows[0]?.id
}

export async function createOrUpdateMemberOrganizations(
  qx: QueryExecutor,
  memberId: string,
  organizationId: string,
  source: string,
  title: string | null | undefined,
  dateStart: string | null | undefined,
  dateEnd: string | null | undefined,
): Promise<string | undefined> {
  if (dateStart) {
    const whereClause = `
      "memberId" = $(memberId)
      AND "title" = $(title)
      AND "organizationId" = $(organizationId)
      AND "dateStart" IS NULL
      AND "dateEnd" IS NULL
    `

    // clean up organizations without dates if we're getting ones with dates
    await qx.result(
      `
          UPDATE "memberOrganizations"
          SET "deletedAt" = NOW()
          WHERE ${whereClause}
        `,
      {
        memberId,
        title,
        organizationId,
      },
    )

    // always clean up affiliation overrides for any organization we soft-delete
    // to prevent stale override data pointing to soft-deleted organizations
    await qx.result(
      `
        DELETE FROM "memberOrganizationAffiliationOverrides"
        WHERE "memberOrganizationId" IN (
          SELECT id FROM "memberOrganizations" WHERE ${whereClause}
        )
      `,
      {
        memberId,
        title,
        organizationId,
      },
    )
  } else {
    const rows = await qx.select(
      `
          SELECT COUNT(*) AS count FROM "memberOrganizations"
          WHERE "memberId" = $(memberId)
          AND "title" = $(title)
          AND "organizationId" = $(organizationId)
          AND "dateStart" IS NOT NULL
          AND "deletedAt" IS NULL
        `,
      {
        memberId,
        title,
        organizationId,
      },
    )
    const row = rows[0] as any
    if (row.count > 0) {
      // if we're getting organization without dates, but there's already one with dates, don't insert
      return
    }
  }

  let conflictCondition = `("memberId", "organizationId", "dateStart", "dateEnd")`
  if (!dateEnd) {
    conflictCondition = `("memberId", "organizationId", "dateStart") WHERE "dateEnd" IS NULL`
  }
  if (!dateStart) {
    conflictCondition = `("memberId", "organizationId") WHERE "dateStart" IS NULL AND "dateEnd" IS NULL`
  }

  const onConflict =
    source === OrganizationSource.UI
      ? `ON CONFLICT ${conflictCondition} DO UPDATE SET "title" = $(title), "dateStart" = $(dateStart), "dateEnd" = $(dateEnd), "deletedAt" = NULL, "source" = $(source)`
      : 'ON CONFLICT DO NOTHING'

  const result = await qx.selectOneOrNone(
    `
        INSERT INTO "memberOrganizations" ("memberId", "organizationId", "createdAt", "updatedAt", "title", "dateStart", "dateEnd", "source")
        VALUES ($(memberId), $(organizationId), NOW(), NOW(), $(title), $(dateStart), $(dateEnd), $(source))
        ${onConflict}
        returning id
      `,
    {
      memberId,
      organizationId,
      title: title || null,
      dateStart: dateStart || null,
      dateEnd: dateEnd || null,
      source: source || null,
    },
  )

  return result?.id
}

export async function updateMemberOrganization(
  qx: QueryExecutor,
  memberId: string,
  id: string,
  data: MemberOrganizationUpdate,
): Promise<IMemberOrganization | undefined> {
  const setClause = Object.keys(data).map((key) => `"${key}" = $(${key})`)
  setClause.push('"updatedAt" = now()')

  const params = { memberId, id, ...data }

  const query = `
    UPDATE "memberOrganizations"
    SET ${setClause.join(', ')}
    WHERE "id" = $(id)
      AND "memberId" = $(memberId)
      AND "deletedAt" IS NULL
    RETURNING *;
  `

  return qx.selectOneOrNone(query, params)
}

export async function deleteMemberOrganizations(
  qx: QueryExecutor,
  memberId: string,
  ids?: string[],
  softDelete = true,
): Promise<void> {
  // Base query depends on soft vs hard delete
  const baseQuery = softDelete
    ? 'UPDATE "memberOrganizations" SET "deletedAt" = NOW()'
    : 'DELETE FROM "memberOrganizations"'

  // Build WHERE clause
  const conditions = ['"memberId" = $(memberId)']
  const params: Record<string, unknown> = { memberId }

  if (ids?.length) {
    conditions.push(`"id" IN ($(ids:csv))`)
    params.ids = ids
  }

  const whereClause = conditions.join(' AND ')
  const query = `${baseQuery} WHERE ${whereClause};`

  await qx.tx(async (tx) => {
    // Capture affected org IDs before the delete — needed for the cleanup step below,
    // since a hard delete removes rows before we can look them up.
    const affectedOrgs: { organizationId: string }[] = await tx.select(
      `SELECT DISTINCT "organizationId" FROM "memberOrganizations" WHERE ${whereClause}`,
      params,
    )
    const affectedOrgIds = affectedOrgs.map((r) => r.organizationId)

    // First delete from memberOrganizationAffiliationOverrides using the same conditions
    await tx.result(
      `DELETE FROM "memberOrganizationAffiliationOverrides"
       WHERE "memberOrganizationId" IN (
         SELECT "id" FROM "memberOrganizations"
         WHERE ${whereClause}
       )`,
      params,
    )

    // Then perform the soft/hard delete on memberOrganizations
    await tx.result(query, params)

    // Clean up segment affiliations for orgs that no longer have any active work experiences
    if (affectedOrgIds.length > 0) {
      await tx.result(
        `DELETE FROM "memberSegmentAffiliations" msa
         WHERE msa."memberId" = $(memberId)
           AND msa."organizationId" IN ($(orgIds:csv))
           AND NOT EXISTS (
             SELECT 1 FROM "memberOrganizations" mo
             WHERE mo."memberId" = $(memberId)
               AND mo."organizationId" = msa."organizationId"
               AND mo."deletedAt" IS NULL
           )`,
        { memberId, orgIds: affectedOrgIds },
      )
    }
  })
}

export async function deleteUndatedMemberOrganizations(
  qx: QueryExecutor,
  memberId: string,
  organizationIds: string[],
): Promise<void> {
  if (organizationIds.length === 0) {
    return
  }

  const whereClause = `
    "memberId" = $(memberId)
    AND "organizationId" IN ($(organizationIds:csv))
    AND "dateStart" IS NULL
    AND "dateEnd" IS NULL
    AND "deletedAt" IS NULL
  `

  const params = { memberId, organizationIds }

  await qx.tx(async (tx) => {
    await tx.result(
      `
        DELETE FROM "memberOrganizationAffiliationOverrides"
        WHERE "memberOrganizationId" IN (
          SELECT "id" FROM "memberOrganizations" WHERE ${whereClause}
        )
      `,
      params,
    )

    await tx.result(
      `
        UPDATE "memberOrganizations"
        SET "deletedAt" = NOW()
        WHERE ${whereClause}
      `,
      params,
    )
  })
}

export async function cleanSoftDeletedMemberOrganization(
  qx: QueryExecutor,
  memberId: string,
  organizationId: string,
  data: Partial<IMemberOrganization>,
): Promise<void> {
  const whereClause = `
    "memberId" = $(memberId)
    AND "organizationId" = $(organizationId)
    AND (("dateStart" = $(dateStart)) OR ("dateStart" IS NULL AND $(dateStart) IS NULL))
    AND (("dateEnd" = $(dateEnd)) OR ("dateEnd" IS NULL AND $(dateEnd) IS NULL))
    AND "deletedAt" IS NOT NULL
  `

  const params = {
    memberId,
    organizationId,
    dateStart: data.dateStart ?? null,
    dateEnd: data.dateEnd ?? null,
  }

  return qx.tx(async (tx) => {
    await tx.result(
      `
        DELETE FROM "memberOrganizationAffiliationOverrides"
        WHERE "memberOrganizationId" IN (
          SELECT "id" FROM "memberOrganizations" WHERE ${whereClause}
        )
      `,
      params,
    )

    await tx.result(
      `
        DELETE FROM "memberOrganizations"
        WHERE ${whereClause}
      `,
      params,
    )
  })
}

export enum EntityField {
  memberId = 'memberId',
  organizationId = 'organizationId',
}

export interface IMergeStrat {
  entityIdField: EntityField
  intersectBasedOnField: EntityField
  entityId(a: IMemberOrganization): string
  intersectBasedOn(a: IMemberOrganization): string
  worthMerging(a: IMemberOrganization, b: IMemberOrganization): boolean
  targetMemberId(role: IMemberOrganization): string
  targetOrganizationId(role: IMemberOrganization): string
}

type RoleToAdd = IMemberOrganization & { originalRoleIds: string[] }

const MemberMergeStrat = (primaryMemberId: string): IMergeStrat => ({
  entityIdField: EntityField.memberId,
  intersectBasedOnField: EntityField.organizationId,
  entityId(role: IMemberOrganization): string {
    return role.memberId
  },
  intersectBasedOn(role: IMemberOrganization): string {
    return role.organizationId
  },
  worthMerging(a: IMemberOrganization, b: IMemberOrganization): boolean {
    return a.organizationId === b.organizationId
  },
  targetMemberId(): string {
    return primaryMemberId
  },
  targetOrganizationId(role: IMemberOrganization): string {
    return role.organizationId
  },
})

const OrgMergeStrat = (primaryOrganizationId: string): IMergeStrat => ({
  entityIdField: EntityField.organizationId,
  intersectBasedOnField: EntityField.memberId,
  entityId(role: IMemberOrganization): string {
    return role.organizationId
  },
  intersectBasedOn(role: IMemberOrganization): string {
    return role.memberId
  },
  worthMerging(a: IMemberOrganization, b: IMemberOrganization): boolean {
    return a.memberId === b.memberId
  },
  targetMemberId(role: IMemberOrganization): string {
    return role.memberId
  },
  targetOrganizationId(): string {
    return primaryOrganizationId
  },
})

export async function findRolesBelongingToBothEntities(
  qx: QueryExecutor,
  primaryId: string,
  secondaryId: string,
  entityIdField: EntityField,
  intersectBasedOnField: EntityField,
): Promise<IMemberOrganization[]> {
  const results = await qx.select(
    `
    SELECT  mo.*
    FROM "memberOrganizations" AS mo
    WHERE mo."deletedAt" is null and
       mo."${intersectBasedOnField}" IN (
        SELECT "${intersectBasedOnField}"
        FROM "memberOrganizations"
        WHERE "${entityIdField}" = $(primaryId)
    )
    AND mo."${intersectBasedOnField}" IN (
        SELECT "${intersectBasedOnField}"
        FROM "memberOrganizations"
        WHERE "${entityIdField}" = $(secondaryId))
    AND mo."${entityIdField}" IN ($(primaryId), $(secondaryId));

  `,
    {
      primaryId,
      secondaryId,
    },
  )

  return results as IMemberOrganization[]
}

export async function findNonIntersectingRoles(
  qx: QueryExecutor,
  primaryId: string,
  secondaryId: string,
  entityIdField: EntityField,
  intersectBasedOnField: EntityField,
): Promise<IMemberOrganization[]> {
  const remainingRoles = (await qx.select(
    `
      SELECT *
      FROM "memberOrganizations"
      WHERE "${entityIdField}" = $(secondaryId)
      AND "deletedAt" IS NULL
      AND "${intersectBasedOnField}" NOT IN (
          SELECT "${intersectBasedOnField}"
          FROM "memberOrganizations"
          WHERE "${entityIdField}" = $(primaryId)
          AND "deletedAt" IS NULL
      );
    `,
    {
      primaryId,
      secondaryId,
    },
  )) as IMemberOrganization[]

  return remainingRoles
}

export async function removeMemberRole(qx: QueryExecutor, role: IMemberOrganization) {
  const conditions = ['"organizationId" = $(organizationId)', '"memberId" = $(memberId)']

  const replacements: Record<string, unknown> = {
    organizationId: role.organizationId,
    memberId: role.memberId,
  }

  if (role.dateStart === null) {
    conditions.push('"dateStart" IS NULL')
  } else {
    conditions.push('"dateStart" = $(dateStart)')
    replacements.dateStart = toIsoString(role.dateStart)
  }

  if (role.dateEnd === null) {
    conditions.push('"dateEnd" IS NULL')
  } else {
    conditions.push('"dateEnd" = $(dateEnd)')
    replacements.dateEnd = toIsoString(role.dateEnd)
  }

  const whereClause = conditions.join(' AND ')

  await qx.tx(async (tx) => {
    // Delete affiliation overrides first using subquery
    await tx.result(
      `
        DELETE FROM "memberOrganizationAffiliationOverrides"
        WHERE "memberOrganizationId" IN (
          SELECT id FROM "memberOrganizations"
          WHERE ${whereClause}
        )
      `,
      replacements,
    )

    // Then delete the role
    await tx.result(
      `
        DELETE FROM "memberOrganizations"
        WHERE ${whereClause}
      `,
      replacements,
    )
  })
}

export async function addMemberRole(
  qx: QueryExecutor,
  role: IMemberOrganization,
): Promise<string | undefined> {
  const query = `
        insert into "memberOrganizations" ("memberId", "organizationId", "createdAt", "updatedAt", "title", "dateStart", "dateEnd", "source")
        values ($(memberId), $(organizationId), NOW(), NOW(), $(title), $(dateStart), $(dateEnd), $(source))
        on conflict do nothing returning id;
  `

  const row = await qx.selectOneOrNone(query, {
    memberId: role.memberId,
    organizationId: role.organizationId,
    title: role.title || null,
    dateStart: role.dateStart,
    dateEnd: role.dateEnd,
    source: role.source || null,
  })

  return row?.id
}

async function moveRolesBetweenEntities(
  qx: QueryExecutor,
  primaryId: string,
  secondaryId: string,
  mergeStrat: IMergeStrat,
  entityType: EntityType,
): Promise<{ shouldRecalculateAffiliations: boolean }> {
  let shouldRecalculateAffiliations = false

  const rolesForBothEntities = await findRolesBelongingToBothEntities(
    qx,
    primaryId,
    secondaryId,
    mergeStrat.entityIdField,
    mergeStrat.intersectBasedOnField,
  )

  const primaryRoles = rolesForBothEntities.filter((m) => mergeStrat.entityId(m) === primaryId)
  const secondaryRoles = rolesForBothEntities.filter((m) => mergeStrat.entityId(m) === secondaryId)

  const findAffiliationOverrides =
    entityType === EntityType.MEMBER
      ? findMemberAffiliationOverrides
      : findOrganizationAffiliationOverrides

  const primaryAffiliationOverrides = await findAffiliationOverrides(qx, primaryId)
  const secondaryAffiliationOverrides = await findAffiliationOverrides(qx, secondaryId)

  const organizationIds = new Set<string>()
  for (const role of rolesForBothEntities) {
    organizationIds.add(role.organizationId)
    organizationIds.add(mergeStrat.targetOrganizationId(role))
  }

  if (entityType === EntityType.ORGANIZATION) {
    organizationIds.add(primaryId)
    organizationIds.add(secondaryId)
  }

  const orgAffiliationPolicyById = await fetchManyOrganizationAffiliationPolicies(qx, [
    ...organizationIds,
  ])

  if (
    entityType === EntityType.ORGANIZATION &&
    (orgAffiliationPolicyById.get(primaryId) || orgAffiliationPolicyById.get(secondaryId))
  ) {
    shouldRecalculateAffiliations = true
  }

  const mergeResult = await mergeRoles(
    qx,
    primaryRoles,
    secondaryRoles,
    primaryAffiliationOverrides,
    secondaryAffiliationOverrides,
    mergeStrat,
    orgAffiliationPolicyById,
  )
  if (mergeResult.shouldRecalculateAffiliations) {
    shouldRecalculateAffiliations = true
  }

  const remainingRoles = await findNonIntersectingRoles(
    qx,
    primaryId,
    secondaryId,
    mergeStrat.entityIdField,
    mergeStrat.intersectBasedOnField,
  )

  // Fetch policies for org IDs not yet in the map (member merge edge case)
  const missingOrgIds = [
    ...new Set(
      remainingRoles
        .flatMap((r) => [r.organizationId, mergeStrat.targetOrganizationId(r)])
        .filter((id) => !orgAffiliationPolicyById.has(id)),
    ),
  ]
  if (missingOrgIds.length > 0) {
    const additional = await fetchManyOrganizationAffiliationPolicies(qx, missingOrgIds)
    for (const [id, blocked] of additional) {
      orgAffiliationPolicyById.set(id, blocked)
    }
  }

  for (const role of remainingRoles) {
    const existingOverride = secondaryAffiliationOverrides.find(
      (o) => o.memberOrganizationId === role.id,
    )

    await removeMemberRole(qx, role)

    const newRoleId = await addMemberRole(qx, {
      title: role.title,
      dateStart: role.dateStart,
      dateEnd: role.dateEnd,
      memberId: mergeStrat.targetMemberId(role),
      organizationId: mergeStrat.targetOrganizationId(role),
      source: role.source,
      deletedAt: role.deletedAt,
    })

    if (!newRoleId) continue

    const targetOrgId = mergeStrat.targetOrganizationId(role)
    const isTargetBlocked = orgAffiliationPolicyById.get(targetOrgId) ?? false
    const isSourceBlocked = orgAffiliationPolicyById.get(role.organizationId) ?? false

    let isPrimaryWorkExp = existingOverride?.isPrimaryWorkExperience ?? false
    if (isPrimaryWorkExp) {
      const alreadyHasIt = primaryAffiliationOverrides.some((o) => o.isPrimaryWorkExperience)
      if (alreadyHasIt) isPrimaryWorkExp = false
    }

    const preserveManualBlock = existingOverride?.allowAffiliation === false && !isSourceBlocked
    const targetMemberId = mergeStrat.targetMemberId(role)
    const shouldWriteOverride = isTargetBlocked || preserveManualBlock || isPrimaryWorkExp
    const finalAllowAffiliation = isTargetBlocked || preserveManualBlock ? false : undefined

    if (shouldWriteOverride) {
      await changeMemberOrganizationAffiliationOverrides(qx, [
        {
          memberId: targetMemberId,
          memberOrganizationId: newRoleId,
          allowAffiliation: finalAllowAffiliation,
          isPrimaryWorkExperience: isPrimaryWorkExp || undefined,
        },
      ])

      // If the affiliation is blocked, delete any existing MSAs to prevent the member from
      // remaining affiliated through a manually created affiliation.
      if (finalAllowAffiliation === false) {
        await deleteMemberSegmentAffiliations(qx, {
          memberId: targetMemberId,
          organizationId: targetOrgId,
        })
      }

      shouldRecalculateAffiliations = true
    }

    if (!isTargetBlocked && existingOverride?.allowAffiliation === false && isSourceBlocked) {
      shouldRecalculateAffiliations = true
    }
  }

  return { shouldRecalculateAffiliations }
}

export async function moveMembersBetweenOrganizations(
  qx: QueryExecutor,
  secondaryOrganizationId: string,
  primaryOrganizationId: string,
): Promise<{ shouldRecalculateAffiliations: boolean }> {
  return moveRolesBetweenEntities(
    qx,
    primaryOrganizationId,
    secondaryOrganizationId,
    OrgMergeStrat(primaryOrganizationId),
    EntityType.ORGANIZATION,
  )
}

export async function moveOrgsBetweenMembers(
  qx: QueryExecutor,
  primaryMemberId: string,
  secondaryMemberId: string,
): Promise<{ shouldRecalculateAffiliations: boolean }> {
  return moveRolesBetweenEntities(
    qx,
    primaryMemberId,
    secondaryMemberId,
    MemberMergeStrat(primaryMemberId),
    EntityType.MEMBER,
  )
}

export async function mergeRoles(
  qx: QueryExecutor,
  primaryRoles: IMemberOrganization[],
  secondaryRoles: IMemberOrganization[],
  primaryAffiliationOverrides: IMemberOrganizationAffiliationOverride[],
  secondaryAffiliationOverrides: IMemberOrganizationAffiliationOverride[],
  mergeStrat: IMergeStrat,
  orgAffiliationPolicyById: Map<string, boolean>,
): Promise<{ shouldRecalculateAffiliations: boolean }> {
  const isDefinedId = (id: string | undefined): id is string => !!id

  const areDatesEqual = (a: Date | string | null, b: Date | string | null): boolean => {
    if (a === null && b === null) return true
    if (a === null || b === null) return false
    return new Date(a).getTime() === new Date(b).getTime()
  }

  // matches the db unique key — title excluded because ON CONFLICT ignores it
  const isSameRole = (a: IMemberOrganization, b: IMemberOrganization): boolean =>
    a.memberId === b.memberId &&
    a.organizationId === b.organizationId &&
    areDatesEqual(a.dateStart, b.dateStart) &&
    areDatesEqual(a.dateEnd, b.dateEnd)

  const toTargetEntity = (role: IMemberOrganization): RoleToAdd => ({
    title: role.title,
    dateStart: role.dateStart,
    dateEnd: role.dateEnd,
    memberId: mergeStrat.targetMemberId(role),
    organizationId: mergeStrat.targetOrganizationId(role),
    source: role.source,
    originalRoleIds: role.id ? [role.id] : [],
  })

  let shouldRecalculateAffiliations = false
  const removedIds = new Set<string>()
  const removeRoles: IMemberOrganization[] = []
  const addRoles: RoleToAdd[] = []
  const overridesToRecreate: {
    role: IMemberOrganization
    override: IMemberOrganizationAffiliationOverride
  }[] = []

  const allExistingOverrides = [...primaryAffiliationOverrides, ...secondaryAffiliationOverrides]

  const queueRoleRemoval = (role: IMemberOrganization) => {
    if (role.id && !removedIds.has(role.id)) {
      removedIds.add(role.id)
      removeRoles.push(role)
    }
  }

  // finds primary dated roles overlapping with a secondary, skipping already-removed ones
  const getOverlaps = (secondary: IMemberOrganization) => {
    if (!secondary.dateStart || !secondary.dateEnd) return []

    const sStart = new Date(secondary.dateStart)
    const sEnd = new Date(secondary.dateEnd)

    return primaryRoles.filter((p) => {
      if ((p.id && removedIds.has(p.id)) || !p.dateStart || !p.dateEnd) return false
      if (mergeStrat.intersectBasedOn(p) !== mergeStrat.intersectBasedOn(secondary)) return false
      return sStart < new Date(p.dateEnd) && new Date(p.dateStart) < sEnd
    })
  }

  // Phase 1: planning — decide which roles to remove and what to add
  for (const secondary of secondaryRoles) {
    const { dateStart, dateEnd } = secondary

    // Case A: undated — no-op insert just to carry overrides to primary's existing role
    if (!dateStart && !dateEnd) {
      const match = primaryRoles.find(
        (p) => mergeStrat.worthMerging(p, secondary) && !p.dateStart && !p.dateEnd,
      )
      if (match) {
        addRoles.push({
          ...toTargetEntity(match),
          originalRoleIds: [match.id, secondary.id].filter(isDefinedId),
        })
      }
      queueRoleRemoval(secondary)
    }

    // Case B: current / ongoing roles (start date, no end date) — keep earliest start date
    else if (dateStart && !dateEnd) {
      const currentPrimaries = primaryRoles.filter(
        (p) =>
          mergeStrat.worthMerging(p, secondary) &&
          p.dateStart &&
          !p.dateEnd &&
          !(p.id && removedIds.has(p.id)),
      )

      if (currentPrimaries.length === 0) {
        const existingCurrentAdd = addRoles.find(
          (r) =>
            r.dateStart &&
            !r.dateEnd &&
            mergeStrat.intersectBasedOn(r) === mergeStrat.intersectBasedOn(secondary),
        )
        if (existingCurrentAdd) {
          if (new Date(secondary.dateStart) < new Date(existingCurrentAdd.dateStart)) {
            existingCurrentAdd.dateStart = new Date(secondary.dateStart).toISOString()
          }
          if (secondary.id) existingCurrentAdd.originalRoleIds.push(secondary.id)
        } else {
          addRoles.push(toTargetEntity(secondary))
        }
      } else {
        const existingAdd = addRoles.find((r) =>
          currentPrimaries.some((p) => p.id && r.originalRoleIds.includes(p.id)),
        )

        const earliestStart = new Date(
          Math.min(...[...currentPrimaries, secondary].map((r) => new Date(r.dateStart).getTime())),
        ).toISOString()

        if (existingAdd) {
          if (!existingAdd.dateStart || new Date(earliestStart) < new Date(existingAdd.dateStart)) {
            existingAdd.dateStart = earliestStart
          }
          if (secondary.id) existingAdd.originalRoleIds.push(secondary.id)
          for (const p of currentPrimaries) {
            if (p.id && !existingAdd.originalRoleIds.includes(p.id)) {
              existingAdd.originalRoleIds.push(p.id)
              queueRoleRemoval(p)
            }
          }
        } else {
          addRoles.push({
            ...toTargetEntity(currentPrimaries[0]),
            dateStart: earliestStart,
            originalRoleIds: [...currentPrimaries.map((p) => p.id), secondary.id].filter(
              isDefinedId,
            ),
          })
          currentPrimaries.forEach(queueRoleRemoval)
        }
      }
      queueRoleRemoval(secondary)
    }

    // Case C: dated / historical roles — merge overlapping ranges into one
    else if (dateStart && dateEnd) {
      const intersecting = getOverlaps(secondary)

      const existingAdd = addRoles.find(
        (r) =>
          r.dateStart &&
          r.dateEnd &&
          mergeStrat.intersectBasedOn(r) === mergeStrat.intersectBasedOn(secondary) &&
          new Date(secondary.dateStart) < new Date(r.dateEnd) &&
          new Date(r.dateStart) < new Date(secondary.dateEnd),
      )

      if (existingAdd) {
        const allNew = [...intersecting, secondary]
        const minStart = Math.min(
          new Date(existingAdd.dateStart).getTime(),
          ...allNew.map((r) => new Date(r.dateStart as Date | string).getTime()),
        )
        const maxEnd = Math.max(
          new Date(existingAdd.dateEnd).getTime(),
          ...allNew.map((r) => new Date(r.dateEnd as Date | string).getTime()),
        )
        existingAdd.dateStart = new Date(minStart).toISOString()
        existingAdd.dateEnd = new Date(maxEnd).toISOString()
        for (const r of allNew) {
          if (r.id) existingAdd.originalRoleIds.push(r.id)
        }
      } else {
        const allMatching = [...intersecting, secondary]
        const base = intersecting[0] || secondary
        addRoles.push({
          ...toTargetEntity(base),
          dateStart: new Date(
            Math.min(...allMatching.map((r) => new Date(r.dateStart as Date | string).getTime())),
          ).toISOString(),
          dateEnd: new Date(
            Math.max(...allMatching.map((r) => new Date(r.dateEnd as Date | string).getTime())),
          ).toISOString(),
          originalRoleIds: allMatching.map((r) => r.id).filter(isDefinedId),
        })
      }

      intersecting.forEach(queueRoleRemoval)
      queueRoleRemoval(secondary)
    }

    // Case D: invalid data (dateEnd without dateStart) — just clean up
    else {
      queueRoleRemoval(secondary)
    }
  }

  // Phase 2: execute removals, saving any overrides so they can be re-applied
  for (const role of removeRoles) {
    const override = allExistingOverrides.find((o) => o.memberOrganizationId === role.id)
    if (override) overridesToRecreate.push({ role, override })
    await removeMemberRole(qx, role)
  }

  // Phase 3: insert new roles and resolve override policies
  for (const addData of addRoles) {
    const newId = await addMemberRole(qx, addData)
    const primaryMatch = !newId ? primaryRoles.find((p) => isSameRole(p, addData)) : null
    const targetRoleId = newId || primaryMatch?.id

    if (!targetRoleId) continue

    const relevant = overridesToRecreate.filter((item) =>
      item.role.id ? addData.originalRoleIds.includes(item.role.id) : false,
    )
    const targetOrgBlocked = orgAffiliationPolicyById.get(addData.organizationId) ?? false

    // keep isPrimaryWorkExperience if primary had it, or adopt from secondary when vacant
    const primaryOverride =
      primaryAffiliationOverrides.find((o) => o.memberOrganizationId === targetRoleId) ??
      relevant.find((item) =>
        primaryAffiliationOverrides.some((o) => o.memberOrganizationId === item.role.id),
      )?.override

    const secondaryHasExp = relevant.some(
      (r) =>
        r.override.isPrimaryWorkExperience &&
        secondaryAffiliationOverrides.some((s) => s.memberOrganizationId === r.role.id),
    )
    const otherPrimaryHasExp = primaryAffiliationOverrides.some(
      (o) => o.isPrimaryWorkExperience && o.memberOrganizationId !== targetRoleId,
    )
    const finalIsPrimaryWorkExp = !!(
      primaryOverride?.isPrimaryWorkExperience ||
      (secondaryHasExp && !otherPrimaryHasExp)
    )

    // block if target org is blocked, primary was blocked, or secondary had a manual block
    const secondaryManualBlock = relevant.some(
      (r) =>
        r.override.allowAffiliation === false &&
        !orgAffiliationPolicyById.get(r.role.organizationId),
    )
    const finalAllowAffiliation =
      targetOrgBlocked || primaryOverride?.allowAffiliation === false || secondaryManualBlock
        ? false
        : undefined

    if (
      targetOrgBlocked ||
      primaryOverride?.allowAffiliation === false ||
      secondaryManualBlock ||
      finalIsPrimaryWorkExp
    ) {
      await changeMemberOrganizationAffiliationOverrides(qx, [
        {
          memberId: addData.memberId,
          memberOrganizationId: targetRoleId,
          allowAffiliation: finalAllowAffiliation,
          isPrimaryWorkExperience: finalIsPrimaryWorkExp || undefined,
        },
      ])
      if (finalAllowAffiliation === false) {
        await deleteMemberSegmentAffiliations(qx, {
          memberId: addData.memberId,
          organizationId: addData.organizationId,
        })
      }
      shouldRecalculateAffiliations = true
    }

    // secondary had allowAffiliation=false from an org-level block (not manual) —
    // that block doesn't carry over, but affiliations need recalculation since the
    // previously suppressed affiliation may now be valid
    if (!shouldRecalculateAffiliations && !targetOrgBlocked) {
      const hadOrgLevelBlock = relevant.some(
        (r) =>
          r.override.allowAffiliation === false &&
          secondaryAffiliationOverrides.some((s) => s.memberOrganizationId === r.role.id) &&
          (orgAffiliationPolicyById.get(r.role.organizationId) ?? false),
      )
      if (hadOrgLevelBlock) shouldRecalculateAffiliations = true
    }
  }

  return { shouldRecalculateAffiliations }
}

export async function fetchMemberWorkExperienceWithEpochDates(
  qx: QueryExecutor,
  batchSize: number,
): Promise<IMemberOrganization[]> {
  const result = await qx.select(
    `
    SELECT id, "memberId", "organizationId", "dateStart", "dateEnd", "title", "source"
    FROM "memberOrganizations"
    WHERE (
      "dateStart" = '1970-01-01 00:00:00+00'::timestamptz
      OR "dateEnd" = '1970-01-01 00:00:00+00'::timestamptz
    )
    AND "deletedAt" IS NULL
    ORDER BY "id" ASC
    LIMIT $(batchSize);
    `,
    { batchSize },
  )

  return result
}
