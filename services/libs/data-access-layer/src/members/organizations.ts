import {
  IMemberOrganization,
  IMemberOrganizationAffiliationOverride,
  IMemberRoleWithOrganization,
  MemberOrganizationUpdate,
  OrganizationSource,
} from '@crowd/types'

import {
  changeMemberOrganizationAffiliationOverrides,
  findMemberAffiliationOverrides,
  findOrganizationAffiliationOverrides,
} from '../member-organization-affiliation'
import { EntityType } from '../old/apps/script_executor_worker/types'
import { QueryExecutor } from '../queryExecutor'

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

export async function fetchMemberOrganizationsBySource(
  qx: QueryExecutor,
  memberId: string,
  source: OrganizationSource,
): Promise<IMemberOrganization[]> {
  return qx.select(
    `
      SELECT "id", "organizationId", "dateStart", "dateEnd", "title", "memberId", "source"
      FROM "memberOrganizations"
      WHERE "memberId" = $(memberId)
        AND "source" = $(source)
        AND "deletedAt" IS NULL
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
): Promise<Map<string, IMemberRoleWithOrganization[]>> {
  const memberRoles = (await qx.select(
    `
      SELECT mo.*, o."displayName" as "organizationName", o.logo as "organizationLogo"
      FROM "memberOrganizations" mo
      join "organizations" o on mo."organizationId" = o.id
      WHERE mo."memberId" in ($(memberIds:csv))
      AND mo."deletedAt" IS NULL;
    `,
    {
      memberIds,
    },
  )) as IMemberRoleWithOrganization[]

  const resultMap = new Map<string, IMemberRoleWithOrganization[]>()
  for (const memberId of memberIds) {
    const roles = memberRoles.filter((r) => r.memberId === memberId)
    resultMap.set(memberId, roles)
  }

  return resultMap
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

export async function createMemberOrganization(
  qx: QueryExecutor,
  memberId: string,
  data: Partial<IMemberOrganization>,
): Promise<string | undefined> {
  const result = await qx.selectOneOrNone(
    `
      INSERT INTO "memberOrganizations"(
        "memberId",
        "organizationId",
        "dateStart",
        "dateEnd",
        "title",
        "source",
        "verified",
        "verifiedBy",
        "createdAt",
        "updatedAt"
      )
      VALUES(
        $(memberId),
        $(organizationId),
        $(dateStart),
        $(dateEnd),
        $(title),
        $(source),
        $(verified),
        $(verifiedBy),
        now(),
        now()
      )
      ON CONFLICT DO NOTHING
      RETURNING id
    `,
    {
      memberId,
      organizationId: data.organizationId,
      dateStart: data.dateStart ?? null,
      dateEnd: data.dateEnd ?? null,
      title: data.title ?? null,
      source: data.source ?? null,
      verified: data.verified ?? false,
      verifiedBy: data.verifiedBy ?? null,
    },
  )

  return result?.id
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

type RoleToAdd = IMemberOrganization & { originalRoleIds?: string[] }

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

    if (isTargetBlocked) {
      await changeMemberOrganizationAffiliationOverrides(qx, [
        {
          memberId: mergeStrat.targetMemberId(role),
          memberOrganizationId: newRoleId,
          allowAffiliation: false,
          isPrimaryWorkExperience: isPrimaryWorkExp || undefined,
        },
      ])
      shouldRecalculateAffiliations = true
    } else if (preserveManualBlock || isPrimaryWorkExp) {
      await changeMemberOrganizationAffiliationOverrides(qx, [
        {
          memberId: mergeStrat.targetMemberId(role),
          memberOrganizationId: newRoleId,
          allowAffiliation: preserveManualBlock ? false : undefined,
          isPrimaryWorkExperience: isPrimaryWorkExp || undefined,
        },
      ])
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

function transformRoleToTargetEntity(
  role: IMemberOrganization,
  mergeStrat: IMergeStrat,
): RoleToAdd {
  return {
    title: role.title,
    dateStart: role.dateStart,
    dateEnd: role.dateEnd,
    memberId: mergeStrat.targetMemberId(role),
    organizationId: mergeStrat.targetOrganizationId(role),
    source: role.source,
    originalRoleIds: role.id ? [role.id] : undefined,
  }
}

function areDatesEqual(dateA: Date | string | null, dateB: Date | string | null): boolean {
  if (dateA === null && dateB === null) return true
  if (dateA === null || dateB === null) return false
  return new Date(dateA).getTime() === new Date(dateB).getTime()
}

function isSamePrimaryRole(a: IMemberOrganization, b: IMemberOrganization): boolean {
  return (
    a.memberId === b.memberId &&
    a.organizationId === b.organizationId &&
    areDatesEqual(a.dateStart, b.dateStart) &&
    areDatesEqual(a.dateEnd, b.dateEnd)
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
  let shouldRecalculateAffiliations = false
  const allExistingOverrides = [...primaryAffiliationOverrides, ...secondaryAffiliationOverrides]
  const removeRoles: IMemberOrganization[] = []
  const addRoles: RoleToAdd[] = []
  const affiliationOverridesToRecreate: {
    role: IMemberOrganization
    override: IMemberOrganizationAffiliationOverride
  }[] = []
  const queueRoleRemoval = (role: IMemberOrganization) => {
    if (role.id && removeRoles.some((r) => r.id === role.id)) {
      return
    }

    removeRoles.push(role)
  }

  // Phase 1: Analyze all secondary roles and build the complete plan
  for (const memberOrganization of secondaryRoles) {
    if (memberOrganization.dateStart === null && memberOrganization.dateEnd === null) {
      queueRoleRemoval(memberOrganization)
    } else if (memberOrganization.dateStart !== null && memberOrganization.dateEnd === null) {
      const currentRoles = primaryRoles.filter(
        (mo) =>
          mergeStrat.worthMerging(mo, memberOrganization) &&
          mo.dateStart !== null &&
          mo.dateEnd === null,
      )

      if (currentRoles.length === 0) {
        addRoles.push(transformRoleToTargetEntity(memberOrganization, mergeStrat))
        queueRoleRemoval(memberOrganization)
      } else if (currentRoles.length === 1) {
        const currentRole = currentRoles[0]
        if (new Date(memberOrganization.dateStart) <= new Date(currentRoles[0].dateStart)) {
          addRoles.push({
            id: currentRole.id,
            dateStart: toIsoString(memberOrganization.dateStart as Date | string),
            dateEnd: null,
            memberId: currentRole.memberId,
            organizationId: currentRole.organizationId,
            title: currentRole.title,
            source: currentRole.source,
            originalRoleIds: [currentRole.id, memberOrganization.id].filter(Boolean),
          })

          queueRoleRemoval(currentRole)
        }

        queueRoleRemoval(memberOrganization)
      } else {
        throw new Error(`Member ${memberOrganization.memberId} has more than one current roles.`)
      }
    } else if (memberOrganization.dateStart === null && memberOrganization.dateEnd !== null) {
      throw new Error(`Member organization with dateEnd and without dateStart!`)
    } else {
      const foundIntersectingRoles = primaryRoles.filter((mo) => {
        const primaryStart = new Date(mo.dateStart)
        const primaryEnd = new Date(mo.dateEnd)
        const secondaryStart = new Date(memberOrganization.dateStart)
        const secondaryEnd = new Date(memberOrganization.dateEnd)

        return (
          mergeStrat.intersectBasedOn(mo) === mergeStrat.intersectBasedOn(memberOrganization) &&
          mo.dateStart !== null &&
          mo.dateEnd !== null &&
          ((secondaryStart < primaryStart && secondaryEnd > primaryStart) ||
            (primaryStart < secondaryStart && secondaryEnd < primaryEnd) ||
            (secondaryStart < primaryStart && secondaryEnd > primaryEnd) ||
            (primaryStart < secondaryStart && secondaryEnd > primaryEnd))
        )
      })

      const startDates = [...foundIntersectingRoles, memberOrganization].map((org) =>
        new Date(org.dateStart).getTime(),
      )
      const endDates = [...foundIntersectingRoles, memberOrganization].map((org) =>
        new Date(org.dateEnd).getTime(),
      )

      addRoles.push({
        dateStart: new Date(Math.min.apply(null, startDates)).toISOString(),
        dateEnd: new Date(Math.max.apply(null, endDates)).toISOString(),
        memberId: mergeStrat.targetMemberId(memberOrganization),
        organizationId: mergeStrat.targetOrganizationId(memberOrganization),
        title:
          foundIntersectingRoles.length > 0
            ? foundIntersectingRoles[0].title
            : memberOrganization.title,
        source:
          foundIntersectingRoles.length > 0
            ? foundIntersectingRoles[0].source
            : memberOrganization.source,
        originalRoleIds: [...foundIntersectingRoles.map((r) => r.id), memberOrganization.id].filter(
          Boolean,
        ),
      })

      for (const r of foundIntersectingRoles) {
        queueRoleRemoval(r)
      }
      queueRoleRemoval(memberOrganization)
    }
  }

  // Phase 2: Execute batch removal of roles
  for (const removeRole of removeRoles) {
    const existingOverride = allExistingOverrides.find(
      (o) => o.memberOrganizationId === removeRole.id,
    )

    if (existingOverride) {
      affiliationOverridesToRecreate.push({
        role: removeRole,
        override: existingOverride,
      })
    }

    await removeMemberRole(qx, removeRole)
  }

  // Phase 3: Execute additions and resolve overrides
  for (const addRole of addRoles) {
    const newRoleId = await addMemberRole(qx, addRole)
    const targetOrgId = mergeStrat.targetOrganizationId(addRole)
    const isTargetBlocked = orgAffiliationPolicyById.get(targetOrgId) ?? false

    // Identify the target ID: either the new one or the existing primary it merged into
    const existingPrimaryRole = !newRoleId
      ? primaryRoles.find((pr) => isSamePrimaryRole(pr, addRole))
      : null
    const targetRoleId = newRoleId || existingPrimaryRole?.id

    if (!targetRoleId) continue

    // 1. Gather all overrides relevant to this specific role merge
    const relevantOverrides = affiliationOverridesToRecreate.filter((item) => {
      return addRole.originalRoleIds?.length
        ? !!item.role.id && addRole.originalRoleIds.includes(item.role.id)
        : item.role.memberId === addRole.memberId && item.role.title === addRole.title
    })

    const removedPrimaryOverride = relevantOverrides.find((item) =>
      primaryAffiliationOverrides.some((o) => o.memberOrganizationId === item.role.id),
    )?.override
    const existingPrimaryOverride = primaryAffiliationOverrides.find(
      (o) => o.memberOrganizationId === targetRoleId,
    )
    const primaryOverride = existingPrimaryOverride ?? removedPrimaryOverride
    const incomingSecondaries = relevantOverrides.filter((r) =>
      secondaryAffiliationOverrides.some((s) => s.memberOrganizationId === r.role.id),
    )

    // 2. Resolve "Primary Work Experience"
    // Keep it if the primary had it, or if a secondary had it and no other primary role claims it.
    const secondaryHasExp = incomingSecondaries.some((s) => s.override.isPrimaryWorkExperience)
    const otherPrimaryHasExp = primaryAffiliationOverrides.some(
      (o) => o.isPrimaryWorkExperience && o.memberOrganizationId !== targetRoleId,
    )
    const finalIsPrimaryWorkExp = !!(
      primaryOverride?.isPrimaryWorkExperience ||
      (secondaryHasExp && !otherPrimaryHasExp)
    )

    // 3. Resolve "Allow Affiliation"
    // Block if the target org is blocked, or preserve row-level manual blocks.
    let finalAllowAffiliation: boolean | undefined = undefined
    const secondaryManualBlock = incomingSecondaries.some(
      (s) =>
        s.override.allowAffiliation === false &&
        !(orgAffiliationPolicyById.get(s.role.organizationId) ?? false),
    )

    if (isTargetBlocked || primaryOverride?.allowAffiliation === false || secondaryManualBlock) {
      finalAllowAffiliation = false
    }

    // 4. Update db
    const needsUpdate =
      isTargetBlocked ||
      primaryOverride?.allowAffiliation === false ||
      secondaryManualBlock ||
      finalIsPrimaryWorkExp

    if (needsUpdate) {
      const effectiveAllowAffiliation = finalAllowAffiliation

      await changeMemberOrganizationAffiliationOverrides(qx, [
        {
          memberId: mergeStrat.targetMemberId(addRole),
          memberOrganizationId: targetRoleId,
          allowAffiliation: effectiveAllowAffiliation,
          isPrimaryWorkExperience: finalIsPrimaryWorkExp || undefined,
        },
      ])
      shouldRecalculateAffiliations = true
    }

    // 5. Recalculate if we intentionally dropped a stale block from a blocked source org.
    if (!shouldRecalculateAffiliations && !isTargetBlocked) {
      if (
        incomingSecondaries.some(
          (s) =>
            s.override.allowAffiliation === false &&
            (orgAffiliationPolicyById.get(s.role.organizationId) ?? false),
        )
      ) {
        shouldRecalculateAffiliations = true
      }
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
