import { partition } from '@crowd/common'
import {
  ILLMConsumableMemberDbResult,
  IMemberMergeSuggestion,
  LLMSuggestionVerdictType,
  MemberMergeSuggestionTable,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export async function upsertMemberMergeSuggestions(
  qx: QueryExecutor,
  suggestions: IMemberMergeSuggestion[],
  similarityThreshold = 0.75,
): Promise<void> {
  if (suggestions.length === 0) {
    return
  }

  const seen = new Set<string>()
  const rows = suggestions
    .filter((suggestion) => {
      const key = suggestion.members.slice().sort().join()
      if (seen.has(key)) {
        return false
      }
      seen.add(key)
      return true
    })
    .map((suggestion) => ({
      memberId: suggestion.members[0],
      toMergeId: suggestion.members[1],
      similarity: suggestion.similarity,
      activityEstimate: suggestion.activityEstimate,
    }))

  const values = `
    SELECT
      unnest($(memberIds)::uuid[]) AS "memberId",
      unnest($(toMergeIds)::uuid[]) AS "toMergeId",
      unnest($(similarities)::double precision[]) AS similarity,
      unnest($(activityEstimates)::integer[]) AS "activityEstimate"
  `

  const pairMatch = `
    (t."memberId" = v."memberId" AND t."toMergeId" = v."toMergeId")
    OR (t."memberId" = v."toMergeId" AND t."toMergeId" = v."memberId")
  `

  for (const chunkRows of partition(rows, 100)) {
    const params = {
      memberIds: chunkRows.map((row) => row.memberId),
      toMergeIds: chunkRows.map((row) => row.toMergeId),
      similarities: chunkRows.map((row) => row.similarity),
      activityEstimates: chunkRows.map((row) => row.activityEstimate),
      similarityThreshold,
    }

    const upsertTable = async (table: MemberMergeSuggestionTable, onlyAboveThreshold: boolean) => {
      const thresholdFilter = onlyAboveThreshold ? 'AND v.similarity > $(similarityThreshold)' : ''

      await qx.result(
        `
          UPDATE "${table}" t
          SET
            similarity = v.similarity,
            "activityEstimate" = v."activityEstimate",
            "updatedAt" = now()
          FROM (${values}) v
          WHERE
            (${pairMatch})
            ${thresholdFilter}
        `,
        params,
      )

      await qx.result(
        `
          INSERT INTO "${table}"
            ("memberId", "toMergeId", "similarity", "activityEstimate", "createdAt", "updatedAt")
          SELECT
            v."memberId",
            v."toMergeId",
            v.similarity,
            v."activityEstimate",
            NOW(),
            NOW()
          FROM (${values}) v
          WHERE EXISTS (SELECT 1 FROM members m WHERE m.id = v."memberId")
            AND EXISTS (SELECT 1 FROM members m WHERE m.id = v."toMergeId")
            AND NOT EXISTS (
              SELECT 1
              FROM "${table}" t
              WHERE ${pairMatch}
            )
            ${thresholdFilter}
        `,
        params,
      )
    }

    await upsertTable(MemberMergeSuggestionTable.MEMBER_TO_MERGE_RAW, false)
    await upsertTable(MemberMergeSuggestionTable.MEMBER_TO_MERGE_FILTERED, true)

    await qx.result(
      `
        DELETE FROM "memberToMerge" t
        USING (${values}) v
        WHERE
          (${pairMatch})
          AND v.similarity <= $(similarityThreshold)
      `,
      params,
    )
  }
}

export async function removeMemberToMerge(
  qx: QueryExecutor,
  memberId: string,
  toMergeId: string,
): Promise<void> {
  await qx.result(
    `
      WITH deleted_filtered AS (
        DELETE FROM "memberToMerge"
        WHERE
          ("memberId" = $(memberId) AND "toMergeId" = $(toMergeId))
          OR
          ("memberId" = $(toMergeId) AND "toMergeId" = $(memberId))
      )
      DELETE FROM "memberToMergeRaw"
      WHERE
        ("memberId" = $(memberId) AND "toMergeId" = $(toMergeId))
        OR
        ("memberId" = $(toMergeId) AND "toMergeId" = $(memberId))
    `,
    { memberId, toMergeId },
  )
}

export async function removeMemberMergeSuggestions(
  qx: QueryExecutor,
  memberId: string,
): Promise<void> {
  await qx.result(
    `
      WITH deleted_filtered AS (
        DELETE FROM "memberToMerge"
        WHERE "memberId" = $(memberId)
           OR "toMergeId" = $(memberId)
      )
      DELETE FROM "memberToMergeRaw"
      WHERE "memberId" = $(memberId)
         OR "toMergeId" = $(memberId)
    `,
    { memberId },
  )
}

export async function findRawMemberMergeSuggestions(
  qx: QueryExecutor,
  similarityFilter: { lte?: number; gte?: number },
  limit: number,
): Promise<string[][]> {
  const rows = await qx.select(
    `
      SELECT mtmr."memberId", mtmr."toMergeId"
      FROM "memberToMergeRaw" mtmr
      WHERE NOT EXISTS (
        SELECT 1
        FROM "llmSuggestionVerdicts" lsv
        WHERE lsv.type = $(type)
          AND (
            (lsv."primaryId" = mtmr."memberId" AND lsv."secondaryId" = mtmr."toMergeId")
            OR (lsv."primaryId" = mtmr."toMergeId" AND lsv."secondaryId" = mtmr."memberId")
          )
      )
      ${similarityFilter.lte ? 'AND mtmr.similarity <= $(similarityLte)' : ''}
      ${similarityFilter.gte ? 'AND mtmr.similarity >= $(similarityGte)' : ''}
      LIMIT $(limit)
    `,
    {
      type: LLMSuggestionVerdictType.MEMBER,
      similarityLte: similarityFilter.lte,
      similarityGte: similarityFilter.gte,
      limit,
    },
  )

  return rows.map((row) => [row.memberId, row.toMergeId])
}

export async function getMembersForLlmMergeSuggestions(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<ILLMConsumableMemberDbResult[]> {
  if (memberIds.length === 0) {
    return []
  }

  return qx.select(
    `
      SELECT
        mem.attributes,
        mem."displayName",
        mem."joinedAt",
        jsonb_agg(DISTINCT jsonb_build_object(
          'platform', mi.platform,
          'value', mi.value,
          'type', mi.type,
          'verified', mi.verified
        )) AS identities,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'displayName', o."displayName",
                'logo', o.logo,
                'dateStart', mo."dateStart",
                'dateEnd', mo."dateEnd",
                'title', mo.title
              )
            )
            FROM "memberOrganizations" mo
            JOIN organizations o ON o.id = mo."organizationId"
            WHERE mo."memberId" = mem.id
          ),
          '[]'::jsonb
        ) AS organizations
      FROM members mem
      JOIN "memberIdentities" mi
        ON mem.id = mi."memberId" AND mi."deletedAt" IS NULL
      WHERE mem.id IN ($(memberIds:csv))
      GROUP BY mem.id, mem.attributes, mem."displayName", mem."joinedAt"
    `,
    { memberIds },
  )
}

export async function findMemberMergeSuggestionsLastGeneratedAt(
  qx: QueryExecutor,
  tenantId: string,
): Promise<string | null> {
  const row = await qx.selectOneOrNone(
    `
      SELECT "memberMergeSuggestionsLastGeneratedAt"
      FROM tenants
      WHERE id = $(tenantId)
    `,
    { tenantId },
  )

  return row?.memberMergeSuggestionsLastGeneratedAt ?? null
}

export async function touchMemberMergeSuggestionsLastGeneratedAt(
  qx: QueryExecutor,
  tenantId: string,
): Promise<void> {
  await qx.result(
    `
      UPDATE tenants
      SET "memberMergeSuggestionsLastGeneratedAt" = now()
      WHERE id = $(tenantId)
    `,
    { tenantId },
  )
}

export async function getMemberNoMerge(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<{ memberId: string; noMergeId: string }[]> {
  const rows = await qx.select(
    `select "memberId", "noMergeId" from "memberNoMerge" where "memberId" in ($(memberIds:csv)) or "noMergeId" in ($(memberIds:csv))`,
    { memberIds },
  )

  return rows
}

export async function insertMemberNoMerge(
  qx: QueryExecutor,
  memberId: string,
  noMergeId: string,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO "memberNoMerge" ("memberId", "noMergeId", "createdAt", "updatedAt")
      VALUES
        ($(memberId), $(noMergeId), NOW(), NOW()),
        ($(noMergeId), $(memberId), NOW(), NOW())
      ON CONFLICT ("memberId", "noMergeId") DO NOTHING
    `,
    { memberId, noMergeId },
  )
}

export async function removeMemberNoMerge(
  qx: QueryExecutor,
  pairs: { memberId: string; noMergeId: string }[],
): Promise<void> {
  if (pairs.length === 0) {
    return
  }

  await qx.result(
    `
      DELETE FROM "memberNoMerge" mnm
      USING (
        SELECT
          unnest($(memberIds)::uuid[]) AS "memberId",
          unnest($(noMergeIds)::uuid[]) AS "noMergeId"
      ) p
      WHERE
        (mnm."memberId" = p."memberId" AND mnm."noMergeId" = p."noMergeId")
        OR (mnm."memberId" = p."noMergeId" AND mnm."noMergeId" = p."memberId")
    `,
    {
      memberIds: pairs.map((pair) => pair.memberId),
      noMergeIds: pairs.map((pair) => pair.noMergeId),
    },
  )
}

export async function suggestMemberMerge(
  qx: QueryExecutor,
  pairs: { members: [string, string]; similarity: number }[],
): Promise<void> {
  if (pairs.length === 0) {
    return
  }

  await qx.tx(async (tx) => {
    await removeMemberNoMerge(
      tx,
      pairs.map(({ members: [memberId, noMergeId] }) => ({ memberId, noMergeId })),
    )
    await upsertMemberMergeSuggestions(
      tx,
      pairs.map(({ members, similarity }) => ({
        similarity,
        members,
        activityEstimate: 0,
      })),
    )
  })
}

export {
  fetchRecentlyOnboardedSubprojects,
  fetchSubprojectMemberMergePairs,
} from './subprojectSuggestions'
