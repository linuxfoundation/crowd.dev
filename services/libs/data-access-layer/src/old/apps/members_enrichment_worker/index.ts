import { generateUUIDv4, redactNullByte } from '@crowd/common'
import { DbConnOrTx, DbStore, DbTransaction } from '@crowd/database'
import {
  IAttributes,
  IEnrichableMember,
  IEnrichmentSourceQueryInput,
  IMemberEnrichmentCache,
  IMemberIdentity,
  IMemberOrganizationData,
  IMemberOriginalData,
  IOrganizationIdentity,
  MemberEnrichmentSource,
  MemberIdentityType,
  OrganizationSource,
} from '@crowd/types'

export async function fetchMemberDataForLLMSquashing(
  db: DbConnOrTx,
  memberId: string,
): Promise<IMemberOriginalData | null> {
  const result = await db.oneOrNone(
    `
    with member_orgs as (select
                            distinct mo."memberId",
                            mo."organizationId" as "orgId",
                            o."displayName"     as "orgName",
                            mo.title            as "jobTitle",
                            mo.id,
                            mo."dateStart",
                            mo."dateEnd",
                            mo.source,
                            jsonb_agg(oi) as identities
                        from "memberOrganizations" mo
                            inner join organizations o on mo."organizationId" = o.id
                            inner join "organizationIdentities" oi on oi."organizationId" = o.id
                        where mo."memberId" = $(memberId)
                          and mo."deletedAt" is null
                          and o."deletedAt" is null
                        group by mo."memberId", mo."organizationId", o."displayName", mo.id)
    select m."displayName",
          m.attributes,
          m."manuallyChangedFields",
          m."tenantId",
          m.reach,
          coalesce(
                  (select json_agg(
                                  (select row_to_json(r)
                                    from (select mi.type,
                                                mi.platform,
                                                mi.value) r)
                          )
                    from "memberIdentities" mi
                    where mi."memberId" = m.id and mi."deletedAt" is null), '[]'::json) as identities,
          case
              when exists (select 1 from member_orgs where "memberId" = m.id)
                  then (
                  select json_agg(
                                  (select row_to_json(r)
                                  from (select mo."orgId",
                                                mo.id,
                                                mo."orgName",
                                                mo."jobTitle",
                                                mo."dateStart",
                                                mo."dateEnd",
                                                mo.source,
                                                mo.identities) r)
                          )
                  from member_orgs mo
                  where mo."memberId" = m.id
              )
              else '[]'::json
              end as organizations
    from members m
    where m.id = $(memberId)
      and m."deletedAt" is null
    group by m.id, m."displayName", m.attributes, m."manuallyChangedFields";
    `,
    {
      memberId,
    },
  )

  return result
}

/**
 * Gets enrichable members using the provided sources
 * If a member is enrichable in one source, and not enrichable in another, the member will be returned
 * Members with at least one missing or old source cache rows will be returned
 * The reason we're not checking enrichable members and cache age in the same subquery is because of linkedin scraper sources.
 * These sources also use data from other sources and it's costly to check cache data jsons.
 * This check is instead done in the application layer.
 */
export async function fetchMembersForEnrichment(
  db: DbStore,
  limit: number,
  sourceInputs: IEnrichmentSourceQueryInput<MemberEnrichmentSource>[],
): Promise<IEnrichableMember[]> {
  const cacheAgeInnerQueryItems = []
  const enrichableBySqlConditions = []

  sourceInputs.forEach((input) => {
    if (input.neverReenrich) {
      cacheAgeInnerQueryItems.push(
        `
      ( NOT EXISTS (
          SELECT 1 FROM "memberEnrichmentCache" mec
          WHERE mec."memberId" = members.id
          AND mec.source = '${input.source}')
      )`,
      )
    } else {
      cacheAgeInnerQueryItems.push(
        `
      ( NOT EXISTS (
          SELECT 1 FROM "memberEnrichmentCache" mec
          WHERE mec."memberId" = members.id
          AND mec.source = '${input.source}'
          AND EXTRACT(EPOCH FROM (now() - mec."updatedAt")) < ${input.cacheObsoleteAfterSeconds})
      )`,
      )
    }

    enrichableBySqlConditions.push(`(${input.enrichableBySql})`)
  })

  let enrichableBySqlJoined = ''

  if (enrichableBySqlConditions.length > 0) {
    enrichableBySqlJoined = `(${enrichableBySqlConditions.join(' OR ')}) `
  }

  return db.connection().query(
    `
    SELECT
         members."id",
         members."displayName",
         members.attributes->'location'->>'default' AS location,
         members.attributes->'websiteUrl'->>'default' AS website,
         JSON_AGG(
           JSON_BUILD_OBJECT(
             'platform', mi.platform,
             'value', mi.value,
             'type', mi.type,
             'verified', mi.verified
           )
         ) AS identities,
         MAX(coalesce("membersGlobalActivityCount".total_count, 0)) AS "activityCount"
    FROM members
         INNER JOIN "memberIdentities" mi ON mi."memberId" = members.id and mi."deletedAt" is null
         LEFT JOIN "membersGlobalActivityCount" ON "membersGlobalActivityCount"."memberId" = members.id
    WHERE
      ${enrichableBySqlJoined}
      AND coalesce((members.attributes ->'isBot'->>'default')::boolean, false) = false 
      AND coalesce((members.attributes ->'isOrganization'->>'default')::boolean, false) = false
      AND members."deletedAt" IS NULL
      AND (${cacheAgeInnerQueryItems.join(' OR ')})
    GROUP BY members.id
    ORDER BY "activityCount" DESC
    LIMIT $1;
    `,
    [limit],
  )
}

export async function fetchMembersForLFIDEnrichment(db: DbStore, limit: number, afterId: string) {
  const idFilter = afterId ? ' and members.id < $2 ' : ''

  return db.connection().query(
    `SELECT
        members."id",
        members."displayName",
        members."attributes",
        members."contributions",
        members."score",
        members."reach",
        jsonb_agg(mi.*) as identities
      FROM members
              INNER JOIN "memberIdentities" mi ON mi."memberId" = members.id
              AND mi."deletedAt" is null
      WHERE (
          (mi.platform = 'github' and mi."sourceId" is not null) OR
          (mi.platform = 'linkedin' and mi."sourceId" is not null) OR
          (mi.platform = 'cvent') OR
          (mi.platform = 'tnc') OR
          (mi.type = 'email' and mi.verified)
          )
        AND members."deletedAt" IS NULL
        ${idFilter}
      GROUP BY members.id
      ORDER BY members.id desc
          limit $1;`,
    [limit, afterId],
  )
}

export async function getIdentitiesExistInOtherMembers(
  db: DbStore,
  excludeMemberId: string,
  identities: IMemberIdentity[],
): Promise<IMemberIdentity[]> {
  if (identities.length === 0) {
    throw new Error(`At least one identity must be provided!`)
  }

  let identityPartialQuery = '('
  const replacements = []
  let replacementIndex = 0

  for (let i = 0; i < identities.length; i++) {
    if (identities[i].type === MemberIdentityType.USERNAME) {
      identityPartialQuery += `(mi.verified and mi.type = '${
        MemberIdentityType.USERNAME
      }' and mi.platform = $${replacementIndex + 1} and mi."value" ilike $${replacementIndex + 2})`
      replacements[replacementIndex] = identities[i].platform
      replacements[replacementIndex + 1] = identities[i].value
      replacementIndex += 2
    } else if (identities[i].type === MemberIdentityType.EMAIL) {
      identityPartialQuery += `(mi.verified and mi.type = '${
        MemberIdentityType.EMAIL
      }' and mi."value" ilike $${replacementIndex + 1})`
      replacements[replacementIndex] = identities[i].value
      replacementIndex += 1
    }

    if (i !== identities.length - 1) {
      identityPartialQuery += ' OR '
    }
  }
  identityPartialQuery += ')'

  // push replacement for excluded member id to the end of replacements array
  replacements.push(excludeMemberId)

  const query = `select * from "memberIdentities" mi
  where ${identityPartialQuery} and mi."deletedAt" is null
  and mi."memberId" <> $${replacementIndex + 1};`

  return db.connection().query(query, replacements)
}

export async function getGithubIdentitiesWithoutSourceId(
  db: DbStore,
  limit: number,
  afterId: string,
  afterValue: string,
): Promise<IMemberIdentity[]> {
  if (afterId) {
    return db.connection().query(
      `select * from "memberIdentities" mi
      where mi.platform = 'github' and mi."sourceId" is null and mi."deletedAt" is null
      and (
        mi."memberId" < $1 OR
        (mi."memberId" = $1 AND mi."value" < $2)
      )
      order by mi."memberId" desc
      limit $3;`,
      [afterId, afterValue, limit],
    )
  }

  return db.connection().query(
    `select * from "memberIdentities" mi
    where mi.platform = 'github' and mi."sourceId" is null and mi."deletedAt" is null
    order by mi."memberId" desc
    limit $1;`,
    [limit],
  )
}

export async function updateIdentitySourceId(
  db: DbStore,
  identity: IMemberIdentity,
  sourceId: string,
) {
  await db
    .connection()
    .query(
      `UPDATE "memberIdentities" SET "sourceId" = $1 WHERE "memberId" = $2 AND platform = $3 AND value = $4 AND "deletedAt" is null;`,
      [sourceId, identity.memberId, identity.platform, identity.value],
    )
}

export async function setMemberEnrichmentLastTriedAt(
  tx: DbConnOrTx,
  memberId: string,
): Promise<void> {
  await tx.none(
    `
    insert into "memberEnrichments"("memberId", "lastTriedAt")
    values ($(memberId), now())
    on conflict ("memberId") do update set "lastTriedAt" = now()
    `,
    { memberId },
  )
}

export async function setMemberEnrichmentUpdatedAt(
  tx: DbConnOrTx,
  memberId: string,
): Promise<void> {
  await tx.none(
    `
    insert into "memberEnrichments"("memberId", "lastTriedAt", "lastUpdatedAt")
    values ($(memberId), now(), now())
    on conflict ("memberId") do update set "lastUpdatedAt" = now()
    `,
    { memberId },
  )
}

export async function findExistingMember(
  db: DbStore,
  memberId: string,
  platform: string,
  values: string[],
  type: MemberIdentityType,
): Promise<string[]> {
  const results = await db.connection().any(
    `SELECT mi."memberId"
       FROM "memberIdentities" mi
       WHERE mi."deletedAt" is null
         AND mi."memberId" <> $(memberId)
         AND mi.platform = $(platform)
         AND mi.value in ($(values:csv))
         AND mi.type = $(type)
         AND EXISTS (SELECT 1 FROM "memberSegments" ms WHERE ms."memberId" = mi."memberId")`,
    {
      memberId,
      platform,
      values,
      type,
    },
  )

  return results.map((r) => r.memberId)
}

export async function addMemberToMerge(tx: DbTransaction, memberId: string, toMergeId: string) {
  await tx.query(
    `INSERT INTO "memberToMerge" ("memberId", "toMergeId", similarity)
                VALUES ($1, $2, $3);"`,
    [memberId, toMergeId, 0.9],
  )
}

export async function findOrganizationIdentities(
  tx: DbTransaction,
  organizationId: string,
): Promise<IOrganizationIdentity[]> {
  return tx.any(
    `select * from "organizationIdentities" where "organizationId" = $(organizationId);`,
    {
      organizationId,
    },
  )
}

export async function findOrganizationByVerifiedIdentity(
  tx: DbTransaction,
  tenantId: string,
  identity: IOrganizationIdentity,
): Promise<string | null> {
  const result = await tx.oneOrNone(
    `
    select oi."organizationId"
    from "organizationIdentities" oi
    where 
      oi."tenantId" = $(tenantId)
      and oi.platform = $(platform)
      and oi.value ilike $(value)
      and oi.type = $(type)
      and oi.verified = true
    limit 1
    `,
    { tenantId, value: identity.value, platform: identity.platform, type: identity.type },
  )

  if (result) {
    return result.organizationId
  }

  return null
}

export async function insertOrganization(
  tx: DbTransaction,
  tenantId: string,
  company: string,
  location: string,
): Promise<string> {
  const id = generateUUIDv4()
  await tx.none(
    `INSERT INTO organizations (id, "tenantId", "displayName", location, "createdAt", "updatedAt")
     VALUES ($(id), $(tenantId), $(displayName), $(location), now(), now())`,
    {
      id,
      tenantId,
      displayName: company,
      location,
    },
  )

  return id
}

export async function updateOrgIdentity(
  tx: DbTransaction,
  organizationId: string,
  tenantId: string,
  identity: IOrganizationIdentity,
): Promise<void> {
  await tx.none(
    `
    update "organizationIdentities" set
      verified = true
    where "organizationId" = $(organizationId) and "tenantId" = $(tenantId) and platform = $(platform) and value = $(value) and type = $(type)
  `,
    {
      organizationId,
      tenantId,
      platform: identity.platform,
      value: identity.value,
      type: identity.type,
    },
  )
}

export async function deleteMemberOrgById(tx: DbTransaction, id: string): Promise<void> {
  // Execute directly on the provided transaction to avoid creating nested savepoints
  await tx.none(
    `
      DELETE FROM "memberOrganizationAffiliationOverrides"
      WHERE "memberOrganizationId" = $(id);
    `,
    { id },
  )

  await tx.none(
    `
      UPDATE "memberOrganizations"
      SET "deletedAt" = NOW()
      WHERE id = $(id);
    `,
    { id },
  )
}

export async function findMemberOrgs(db: DbStore, memberId: string, orgId: string) {
  return await db.connection().query(
    `SELECT COUNT(*) AS count FROM "memberOrganizations"
                WHERE "memberId" = $1
                AND "organizationId" = $2
                AND "dateStart" IS NOT NULL
                AND "deletedAt" IS NULL
              `,
    [memberId, orgId],
  )
}

export async function updateMemberOrg(
  tx: DbTransaction,
  memberId: string,
  original: IMemberOrganizationData,
  toUpdate: Record<string, unknown>,
): Promise<string | null> {
  const keys = Object.keys(toUpdate)
  if (keys.length === 0) {
    return null
  }

  // First check if another row like this exists so that we don't get unique index violations.
  // We compute the "target" state after applying toUpdate to decide what to look for.
  const params = {
    memberId,
    id: original.id,
    organizationId: original.orgId,
    // Use updated value if provided, otherwise keep original
    dateStart: toUpdate.dateStart !== undefined ? toUpdate.dateStart : original.dateStart,
    dateEnd: toUpdate.dateEnd !== undefined ? toUpdate.dateEnd : original.dateEnd,
  }

  let dateEndFilter = `and "dateEnd" = $(dateEnd)`
  let dateStartFilter = `and "dateStart" = $(dateStart)`

  if (params.dateEnd === null) {
    dateEndFilter = `and "dateEnd" is null`
    delete params.dateEnd
  }

  if (params.dateStart === null) {
    dateStartFilter = ` and "dateStart" is null`
    delete params.dateStart
  }

  const existing = await tx.oneOrNone(
    `
      select 1 from "memberOrganizations"
      where "memberId" = $(memberId)
      and "organizationId" = $(organizationId)
      ${dateStartFilter}
      ${dateEndFilter}
      and "deletedAt" is null
      and id <> $(id)
    `,
    params,
  )

  if (existing) {
    // we should just delete the row
    await deleteMemberOrgById(tx, original.id)
    return null
  }

  const sets = keys.map((k) => `"${k}" = $(${k})`)

  const result = await tx.oneOrNone(
    `
      update "memberOrganizations"
      set ${sets.join(',\n')}
      where "memberId" = $(memberId) and id = $(id)
      returning id
      `,
    {
      memberId,
      id: original.id,
      ...toUpdate,
    },
  )

  return result?.id ?? null
}

export async function insertWorkExperience(
  tx: DbTransaction,
  memberId: string,
  orgId: string,
  title: string,
  dateStart: string,
  dateEnd: string,
  source: OrganizationSource,
): Promise<string | null> {
  let conflictCondition = `("memberId", "organizationId", "dateStart", "dateEnd")`
  if (!dateEnd) {
    conflictCondition = `("memberId", "organizationId", "dateStart") WHERE "dateEnd" IS NULL`
  }
  if (!dateStart) {
    conflictCondition = `("memberId", "organizationId") WHERE "dateStart" IS NULL AND "dateEnd" IS NULL`
  }

  const onConflict =
    source === OrganizationSource.UI
      ? `ON CONFLICT ${conflictCondition} DO UPDATE SET "title" = $3, "dateStart" = $4, "dateEnd" = $5, "deletedAt" = NULL, "source" = $6`
      : 'ON CONFLICT DO NOTHING'

  const result = await tx.oneOrNone(
    `
              INSERT INTO "memberOrganizations" ("memberId", "organizationId", "createdAt", "updatedAt", "title", "dateStart", "dateEnd", "source")
              VALUES ($1, $2, NOW(), NOW(), $3, $4, $5, $6)
              ${onConflict}
              RETURNING id;
            `,
    [memberId, orgId, title, dateStart, dateEnd, source],
  )

  return result?.id ?? null
}

export async function updateMember(
  tx: DbTransaction,
  tenantId: string,
  memberId: string,
  displayName: string,
  updateDisplayName: boolean,
  attributes: IAttributes,
  contributions: object,
) {
  let stmtDisplayName = ''
  if (updateDisplayName) {
    stmtDisplayName = `"displayName" = $(displayName),`
  }

  return tx.query(
    `UPDATE members SET ${stmtDisplayName}
      attributes = $(attributes),
      contributions = $(contributions),
      "updatedAt" = NOW()
    WHERE id = $(memberId) AND "tenantId" = $(tenantId);`,
    {
      memberId,
      tenantId,
      attributes,
      contributions: JSON.stringify(contributions),
      displayName,
    },
  )
}

export async function updateMemberAttributes(
  tx: DbTransaction,
  memberId: string,
  attributes: IAttributes,
) {
  return tx.query(
    `UPDATE members SET
      attributes = $1,
      "updatedAt" = NOW()
    WHERE id = $2;`,
    [attributes, memberId],
  )
}

export async function insertMemberEnrichmentCacheDb<T>(
  tx: DbConnOrTx,
  data: T,
  memberId: string,
  source: MemberEnrichmentSource,
) {
  const dataSanitized = data ? redactNullByte(JSON.stringify(data)) : null
  return tx.query(
    `INSERT INTO "memberEnrichmentCache" ("memberId", "data", "createdAt", "updatedAt", "source")
      VALUES ($1, $2, NOW(), NOW(), $3);`,
    [memberId, dataSanitized, source],
  )
}

export async function updateMemberEnrichmentCacheDb<T>(
  tx: DbConnOrTx,
  data: T,
  memberId: string,
  source: MemberEnrichmentSource,
) {
  const dataSanitized = data ? redactNullByte(JSON.stringify(data)) : null
  return tx.query(
    `UPDATE "memberEnrichmentCache"
      SET
        "updatedAt" = NOW(),
        "data" = $2
      WHERE "memberId" = $1 and source = $3;`,
    [memberId, dataSanitized, source],
  )
}

export async function touchMemberEnrichmentCacheUpdatedAtDb(
  tx: DbConnOrTx,
  memberId: string,
  source: MemberEnrichmentSource,
) {
  return tx.query(
    `UPDATE "memberEnrichmentCache"
      SET "updatedAt" = NOW()
      WHERE "memberId" = $1 and source = $2;`,
    [memberId, source],
  )
}

export async function findMemberEnrichmentCacheDb<T>(
  tx: DbConnOrTx,
  memberId: string,
  sources: MemberEnrichmentSource[],
): Promise<IMemberEnrichmentCache<T>[]> {
  const results = await tx.any(
    `
    select *
    from "memberEnrichmentCache"
    where 
      source in ($(sources:csv))
      and "memberId" = $(memberId);
    `,
    { sources, memberId },
  )

  return results
}

export async function findMemberEnrichmentCacheForAllSourcesDb<T>(
  tx: DbConnOrTx,
  memberId: string,
  returnRowsWithoutData = false,
): Promise<IMemberEnrichmentCache<T>[]> {
  const dataFilter = returnRowsWithoutData ? '' : 'and data is not null'
  const result = await tx.manyOrNone(
    `
    select *
    from "memberEnrichmentCache"
    where 
      "memberId" = $(memberId) ${dataFilter};
    `,
    { memberId },
  )

  return result ?? []
}
