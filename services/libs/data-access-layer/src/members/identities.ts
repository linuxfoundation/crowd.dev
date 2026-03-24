import { DEFAULT_TENANT_ID } from '@crowd/common'
import {
  IMemberIdentity,
  MemberIdentityType,
  NewMemberIdentity,
  UpdateMemberIdentity,
} from '@crowd/types'

import { MEMBER_SELECT_COLUMNS } from '../members/base'
import { IDbMember } from '../old/apps/data_sink_worker/repo/member.data'
import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

export async function fetchMemberIdentities(
  qx: QueryExecutor,
  memberId: string,
): Promise<IMemberIdentity[]> {
  return qx.select(
    `
      SELECT *
      FROM "memberIdentities"
      WHERE "memberId" = $(memberId)
        AND "deletedAt" is null
    `,
    {
      memberId,
    },
  )
}

export async function fetchManyMemberIdentities(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; identities: IMemberIdentity[] }[]> {
  return qx.select(
    `
      SELECT
          mi."memberId",
          JSONB_AGG(mi ORDER BY mi."createdAt") AS "identities"
      FROM "memberIdentities" mi
      WHERE mi."memberId" IN ($(memberIds:csv))
        AND mi."deletedAt" is null
      GROUP BY mi."memberId"
    `,
    {
      memberIds,
    },
  )
}

export async function checkIdentityExistance(
  qx: QueryExecutor,
  value: string,
  platform: string,
): Promise<IMemberIdentity[]> {
  return await qx.select(
    `
        SELECT id, "memberId"
        FROM "memberIdentities"
        WHERE "value" = $(value)
          AND "platform" = $(platform)
          AND "deletedAt" is null;
    `,
    {
      value,
      platform,
    },
  )
}

export async function findMemberIdentityById(
  qx: QueryExecutor,
  memberId: string,
  id: string,
): Promise<IMemberIdentity> {
  const res = await qx.select(
    `
        SELECT *
        FROM "memberIdentities"
        WHERE "id" = $(id) 
          AND "memberId" = $(memberId)
          AND "deletedAt" is null;
    `,
    {
      id,
      memberId,
    },
  )

  return res.length > 0 ? res[0] : null
}

export async function findMemberIdentitiesByValue(
  qx: QueryExecutor,
  memberId: string,
  value: string,
  filter: { type?: MemberIdentityType },
): Promise<IMemberIdentity[]> {
  return qx.select(
    `
        SELECT id, platform, "sourceId", type, value, verified
        FROM "memberIdentities"
        WHERE value = $(value) 
          AND "memberId" = $(memberId)
          AND "deletedAt" is null
        ${filter.type ? 'AND type = $(type)' : ''}
    `,
    { value, memberId, type: filter.type },
  )
}

export async function updateMemberIdentity(
  qx: QueryExecutor,
  memberId: string,
  id: string,
  data: Partial<UpdateMemberIdentity>,
): Promise<IMemberIdentity> {
  if (Object.keys(data).length === 0) return null

  const setClause = Object.keys(data).map((key) => `"${key}" = $(${key})`)
  setClause.push('"updatedAt" = now()')

  const params = { memberId, id, ...data }

  const query = `
    UPDATE "memberIdentities"
    SET ${setClause.join(', ')}
    WHERE "memberId" = $(memberId)
      AND "id" = $(id)
      AND "deletedAt" IS NULL
    RETURNING *;
  `

  return qx.selectOneOrNone(query, params)
}

export async function deleteMemberIdentity(
  qx: QueryExecutor,
  memberId: string,
  id: string,
): Promise<number> {
  return qx.result(
    `
        UPDATE "memberIdentities" SET "deletedAt" = now()
        WHERE "memberId" = $(memberId) AND "id" = $(id) AND "deletedAt" is null;
    `,
    {
      memberId,
      id,
    },
  )
}

export async function moveIdentitiesBetweenMembers(
  qx: QueryExecutor,
  fromMemberId: string,
  toMemberId: string,
  identitiesToMove: IMemberIdentity[],
  identitiesToUpdate: IMemberIdentity[],
): Promise<void> {
  for (const i of identitiesToMove) {
    await moveToNewMember(qx, {
      oldMemberId: fromMemberId,
      newMemberId: toMemberId,
      platform: i.platform,
      value: i.value,
      type: i.type,
    })
  }

  if (identitiesToUpdate.length > 0) {
    for (const i of identitiesToUpdate) {
      // first we remove them from the old member (we can't update and delete at the same time because of a unique index where only one identity can have a verified type:value combination for a tenant, member and platform)
      await deleteMemberIdentities(qx, {
        memberId: fromMemberId,
        platform: i.platform,
        value: i.value,
        type: i.type,
      })

      // then we update verified flag for the identities in the new member
      await updateVerifiedFlag(qx, {
        memberId: toMemberId,
        platform: i.platform,
        value: i.value,
        type: i.type,
        verified: true,
      })
    }
  }
}

export async function insertManyMemberIdentities(
  qx: QueryExecutor,
  identities: NewMemberIdentity[],
  failOnConflict = false,
) {
  return qx.result(
    prepareBulkInsert(
      'memberIdentities',
      [
        'memberId',
        'tenantId',
        'integrationId',
        'platform',
        'source',
        'sourceId',
        'value',
        'type',
        'verified',
        'verifiedBy',
      ],
      identities.map((i) => {
        return {
          tenantId: DEFAULT_TENANT_ID,
          ...i,
        }
      }),
      failOnConflict ? undefined : 'DO NOTHING',
    ),
  )
}

export async function createMemberIdentity(
  qx: QueryExecutor,
  i: NewMemberIdentity,
  failOnConflict = false,
) {
  return insertManyMemberIdentities(qx, [i], failOnConflict)
}

export async function moveToNewMember(
  qx: QueryExecutor,
  p: {
    oldMemberId: string
    newMemberId: string
    platform: string
    value: string
    type: MemberIdentityType
  },
) {
  const rowCount = await qx.result(
    `
      update "memberIdentities"
      set
        "memberId" = $(newMemberId)
      where
        "memberId" = $(oldMemberId) and
        platform = $(platform) and
        value = $(value) and
        type = $(type) and
        "deletedAt" is null;
    `,
    p,
  )

  if (rowCount !== 1) {
    throw new Error(
      `Expected 1 row to be updated, but got ${rowCount} when moving identity from ${p.oldMemberId} to ${p.newMemberId}!`,
    )
  }

  return rowCount
}

export async function deleteMemberIdentitiesByCombinations(
  qx: QueryExecutor,
  p: {
    memberId: string
    platforms: string[]
    values: string[]
    types: MemberIdentityType[]
  },
) {
  return qx.result(
    `
      update "memberIdentities" set "deletedAt" = now()
      where ("memberId", platform, value, type) in
            (select mi."memberId", mi.platform, mi.value, mi.type
            from "memberIdentities" mi
                      join (select $(memberId)::uuid            as memberid,
                                  unnest($(platforms)::text[]) as platform,
                                  unnest($(values)::text[]) as value,
                                  unnest($(types)::text[]) as type) as combinations
                          on mi."memberId" = combinations.memberid
                              and mi.platform = combinations.platform
                              and mi.value = combinations.value
                              and mi.type = combinations.type
                              and mi."deletedAt" is null);
    `,
    {
      memberId: p.memberId,
      platforms: `{${p.platforms.join(',')}}`,
      values: `{${p.values.join(',')}}`,
      types: `{${p.types.join(',')}}`,
    },
  )
}

export async function updateVerifiedFlag(
  qx: QueryExecutor,
  p: {
    memberId: string
    platform: string
    value: string
    type: MemberIdentityType
    verified: boolean
  },
) {
  return qx.result(
    `
      update "memberIdentities"
      set verified = $(verified)
      where
        "memberId" = $(memberId) and
        platform = $(platform) and
        value = $(value) and
        type = $(type) and
        "deletedAt" is null
    `,
    p,
  )
}

export async function deleteMemberIdentities(
  qx: QueryExecutor,
  p: { memberId: string; value: string; type: MemberIdentityType; platform: string },
) {
  return qx.result(
    `
      update "memberIdentities" set "deletedAt" = now()
      where "memberId" = $(memberId) 
        and platform = $(platform) 
        and value = $(value) 
        and type = $(type) 
        and "deletedAt" is null;
    `,
    p,
  )
}

export async function deleteManyMemberIdentities(
  qx: QueryExecutor,
  {
    memberId,
    identities,
  }: {
    memberId: string
    identities: {
      platform: string
      value: string
      type: MemberIdentityType
    }[]
  },
) {
  const formattedIdentities = identities
    .map((i) => `('${i.platform}', '${i.value}', '${i.type}')`)
    .join(', ')

  return qx.result(
    `
      update "memberIdentities" set "deletedAt" = now()
      where "memberId" = $(memberId) and
      (platform, value, type) in (${formattedIdentities}) and "deletedAt" is null;
    `,
    {
      memberId,
      formattedIdentities,
    },
  )
}

export async function findAlreadyExistingVerifiedIdentities(
  qx: QueryExecutor,
  p: { identities: IMemberIdentity[] },
): Promise<IMemberIdentity[]> {
  if (p.identities.length === 0) {
    return []
  }

  const valuesClause = p.identities
    .map((_, i) => `($(platform_${i}), $(type_${i}), $(value_${i}))`)
    .join(', ')

  const data: Record<string, string> = {}
  p.identities.forEach((identity, i) => {
    data[`platform_${i}`] = identity.platform
    data[`type_${i}`] = identity.type
    data[`value_${i}`] = identity.value.toLowerCase()
  })

  return qx.select(
    `
    with input_identities (platform, type, value_lower) as (
      values ${valuesClause}
    )
    select mi.*
    from "memberIdentities" mi
    inner join input_identities i
      on mi.platform = i.platform
      and mi.type = i.type
      and lower(mi.value) = i.value_lower
    where mi."deletedAt" is null
    `,
    data,
  )
}

export async function findMembersByVerifiedEmails(
  qx: QueryExecutor,
  emails: string[],
): Promise<Map<string, IDbMember>> {
  if (emails.length === 0) {
    return new Map()
  }

  const valuesClause = emails.map((_, i) => `($(email_${i}))`).join(', ')

  const data: Record<string, string> = {
    type: MemberIdentityType.EMAIL,
  }

  emails.forEach((email, i) => {
    data[`email_${i}`] = email.toLowerCase()
  })

  const results = await qx.select(
    `
    with input_emails (value_lower) as (
      values ${valuesClause}
    )
    select mi.value as "identityValue", ${MEMBER_SELECT_COLUMNS.map((c) => `m."${c}"`).join(', ')}
    from "memberIdentities" mi
    inner join input_emails i on lower(mi.value) = i.value_lower
    inner join "members" m on m.id = mi."memberId"
    where mi.verified = true 
      and mi.type = $(type) 
      and mi."deletedAt" is null
    limit ${emails.length}
  `,
    data,
  )

  const resultMap = new Map<string, IDbMember>()

  for (const result of results) {
    resultMap.set(result.identityValue, result)
  }

  return resultMap
}

export async function findMembersByVerifiedUsernames(
  qx: QueryExecutor,
  params: { segmentId: string; platform: string; username: string }[],
): Promise<Map<{ platform: string; value: string }, IDbMember>> {
  if (params.length === 0) {
    return new Map()
  }

  const valuesClause = params.map((_, i) => `($(platform_${i}), $(username_${i}))`).join(', ')

  const data: Record<string, string> = {
    type: MemberIdentityType.USERNAME,
  }

  params.forEach((param, i) => {
    data[`platform_${i}`] = param.platform
    data[`username_${i}`] = param.username.toLowerCase()
  })

  const results = await qx.select(
    `
      with input_identities (platform, value_lower) as (
        values ${valuesClause}
      )
      select mi.platform as "identityPlatform", mi.value as "identityValue", ${MEMBER_SELECT_COLUMNS.map((c) => `m."${c}"`).join(', ')}
      from "memberIdentities" mi
      inner join input_identities i 
        on mi.platform = i.platform 
        and lower(mi.value) = i.value_lower
      inner join "members" m on m.id = mi."memberId"
      where mi.verified = true 
        and mi.type = $(type) 
        and mi."deletedAt" is null
      limit ${params.length}
    `,
    data,
  )

  const resultMap = new Map<{ platform: string; value: string }, IDbMember>()

  for (const result of results) {
    resultMap.set({ platform: result.identityPlatform, value: result.identityValue }, result)
  }

  return resultMap
}

export async function findMembersByIdentities(
  qx: QueryExecutor,
  identities: IMemberIdentity[],
  memberIdToIgnore?: string,
  onlyVerified = false,
): Promise<Map<string, string>> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const params: any = {}

  const conditions: string[] = []
  if (memberIdToIgnore) {
    conditions.push('mi."memberId" <> $(memberIdToIgnore)')
    params.memberIdToIgnore = memberIdToIgnore
  }

  if (onlyVerified) {
    conditions.push('mi.verified = true')
  }

  const identityParams = identities
    .map((identity) => `('${identity.platform}', '${identity.value}', '${identity.type}')`)
    .join(', ')

  const result = await qx.select(
    `
    with input_identities (platform, value, type) as (
      values ${identityParams}
    )
    select "memberId", i.platform, i.value, i.type
    from "memberIdentities" mi
      inner join input_identities i 
        on mi.platform = i.platform 
        and lower(mi.value) = lower(i.value)
        and mi.type = i.type
        and mi."deletedAt" is null
    where ${conditions.join(' and ')}
  `,
    params,
  )

  const resultMap = new Map<string, string>()
  result.forEach((row) => {
    resultMap.set(`${row.platform}:${row.type}:${row.value}`, row.memberId)
  })

  return resultMap
}

export async function findIdentitiesForMembers(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Map<string, IMemberIdentity[]>> {
  const resultMap = new Map<string, IMemberIdentity[]>()

  const results = await qx.select(
    `
      select * from "memberIdentities"
      where "memberId" in ($(memberIds:csv))
        and "deletedAt" is null
    `,
    {
      memberIds,
    },
  )

  for (const memberId of memberIds) {
    const identities = results.filter((r) => r.memberId === memberId)
    resultMap.set(memberId, identities)
  }

  return resultMap
}

/**
 * Retrieve member IDs matching any of the given identities.
 */
export async function findMemberIdsByIdentities(
  qx: QueryExecutor,
  identities: Partial<IMemberIdentity>[],
): Promise<string[]> {
  if (!identities.length) return []

  const conditions: string[] = []
  const params: Record<string, string> = {}

  identities.forEach((identity, i) => {
    const parts: string[] = []

    Object.entries(identity).forEach(([key, value]) => {
      if (value == null) return

      const paramName = `${key}_${i}`

      // Special handling: lowercase 'value' for case-insensitive match
      if (key === 'value') {
        parts.push(`lower(mi.${key}) = $(${paramName})`)
        params[paramName] = (value as string).toLowerCase()
      } else {
        parts.push(`mi.${key} = $(${paramName})`)
        params[paramName] = value as string
      }
    })

    if (parts.length > 0) {
      conditions.push(`(${parts.join(' AND ')})`)
    }
  })

  if (!conditions.length) return []

  const result = await qx.select(
    `
      SELECT DISTINCT mi."memberId"
      FROM "memberIdentities" mi
      WHERE mi."deletedAt" IS NULL
        AND (${conditions.join(' OR ')})
  `,
    params,
  )

  return result.map((r) => r.memberId)
}
