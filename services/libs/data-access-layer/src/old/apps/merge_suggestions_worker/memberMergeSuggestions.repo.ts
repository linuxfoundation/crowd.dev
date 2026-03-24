import { DbConnection, DbTransaction } from '@crowd/database'
import { Logger } from '@crowd/logging'
import {
  ILLMConsumableMemberDbResult,
  IMemberMergeSuggestion,
  LLMSuggestionVerdictType,
  MemberMergeSuggestionTable,
  SuggestionType,
} from '@crowd/types'

import {
  IFindRawMemberMergeSuggestionsReplacement,
  IMemberId,
  IMemberMergeSuggestionsLatestGeneratedAt,
  IMemberNoMerge,
  IRawMemberMergeSuggestionResult,
} from './types'
import { chunkArray, removeDuplicateSuggestions } from './utils'

class MemberMergeSuggestionsRepository {
  constructor(
    private readonly connection: DbConnection | DbTransaction,
    private readonly log: Logger,
  ) {}

  async findNoMergeIds(memberId: string): Promise<string[]> {
    try {
      const results: IMemberNoMerge[] = await this.connection.any(
        `select mnm."memberId", mnm."noMergeId" from "memberNoMerge" mnm
        where mnm."memberId" = $(id) or mnm."noMergeId" = $(id);`,
        {
          id: memberId,
        },
      )
      return Array.from(
        results.reduce((acc, r) => {
          if (memberId === r.memberId) {
            acc.add(r.noMergeId)
          } else if (memberId === r.noMergeId) {
            acc.add(r.memberId)
          }
          return acc
        }, new Set<string>()),
      )
    } catch (err) {
      throw new Error(err)
    }
  }

  async findTenantsLatestMemberSuggestionGeneratedAt(tenantId: string): Promise<string> {
    try {
      const result: IMemberMergeSuggestionsLatestGeneratedAt = await this.connection.oneOrNone(
        `
        select "memberMergeSuggestionsLastGeneratedAt"
        from tenants
        where "id" = $(tenantId);`,
        {
          tenantId,
        },
      )
      return result?.memberMergeSuggestionsLastGeneratedAt
    } catch (err) {
      throw new Error(err)
    }
  }

  async updateMemberMergeSuggestionsLastGeneratedAt(tenantId: string): Promise<void> {
    try {
      await this.connection.any(
        `
          update tenants set "memberMergeSuggestionsLastGeneratedAt" = now()
          where "id" = $(tenantId);
        `,
        {
          tenantId,
        },
      )
    } catch (err) {
      throw new Error(err)
    }
  }

  async addToMerge(
    suggestions: IMemberMergeSuggestion[],
    table: MemberMergeSuggestionTable,
  ): Promise<void> {
    // Remove possible duplicates
    suggestions = removeDuplicateSuggestions<IMemberMergeSuggestion>(
      suggestions,
      SuggestionType.MEMBERS,
    )

    // check all suggestion ids exists in the db
    const uniqueMemberIds = Array.from(
      suggestions.reduce((acc, suggestion) => {
        acc.add(suggestion.members[0])
        acc.add(suggestion.members[1])
        return acc
      }, new Set<string>()),
    )

    // filter non existing member ids from suggestions
    const nonExistingIds = await this.findNonExistingIds(uniqueMemberIds)

    suggestions = suggestions.filter(
      (s) => !nonExistingIds.includes(s.members[0]) && !nonExistingIds.includes(s.members[1]),
    )

    // Process suggestions in chunks of 100 or less
    const suggestionChunks: IMemberMergeSuggestion[][] = chunkArray<IMemberMergeSuggestion>(
      suggestions,
      100,
    )

    const insertValues = (
      memberId: string,
      toMergeId: string,
      similarity: number | null,
      activityEstimate: number | null,
      index: number,
    ) => {
      const idPlaceholder = (key: string) => `${key}${index}`
      return {
        query: `($(${idPlaceholder('memberId')})::uuid, $(${idPlaceholder(
          'toMergeId',
        )})::uuid, $(${idPlaceholder('similarity')}), $(${idPlaceholder(
          'activityEstimate',
        )})::integer, NOW(), NOW())`,
        replacements: {
          [idPlaceholder('memberId')]: memberId,
          [idPlaceholder('toMergeId')]: toMergeId,
          [idPlaceholder('similarity')]: similarity,
          [idPlaceholder('activityEstimate')]: activityEstimate,
        },
      }
    }

    for (const suggestionChunk of suggestionChunks) {
      const placeholders: string[] = []
      let replacements: Record<string, unknown> = {}

      suggestionChunk.forEach((suggestion, index) => {
        const { query, replacements: chunkReplacements } = insertValues(
          suggestion.members[0],
          suggestion.members[1],
          suggestion.similarity,
          suggestion.activityEstimate,
          index,
        )
        placeholders.push(query)
        replacements = { ...replacements, ...chunkReplacements }
      })

      try {
        // 1. Update existing rows if they already exist
        await this.connection.none(
          `
            update "${table}" t
            set 
              similarity = v.similarity,
              "activityEstimate" = v."activityEstimate",
              "updatedAt" = now()
            from (
              values
                ${placeholders.join(', ')}
            ) as v("memberId", "toMergeId", "similarity", "activityEstimate", "createdAt", "updatedAt")
            where 
              (t."memberId" = v."memberId"::uuid and t."toMergeId" = v."toMergeId"::uuid)
              or
              (t."memberId" = v."toMergeId"::uuid and t."toMergeId" = v."memberId"::uuid);
        `,
          replacements,
        )

        // 2. Insert only new rows and enforce bidirectional uniqueness
        await this.connection.none(
          `
            insert into "${table}" 
              ("memberId", "toMergeId", "similarity", "activityEstimate", "createdAt", "updatedAt")
            select v.*
            from (
              values
                ${placeholders.join(', ')}
            ) as v("memberId", "toMergeId", "similarity", "activityEstimate", "createdAt", "updatedAt")
            where not exists (
              select 1
              from "${table}" t
              where 
                (t."memberId" = v."memberId"::uuid and t."toMergeId" = v."toMergeId"::uuid)
                or
                (t."memberId" = v."toMergeId"::uuid and t."toMergeId" = v."memberId"::uuid)
            );
        `,
          replacements,
        )
      } catch (error) {
        this.log.error('Error adding members to merge', error)
        throw error
      }
    }
  }

  async findNonExistingIds(memberIds: string[]): Promise<string[]> {
    let idValues = ``

    for (let i = 0; i < memberIds.length; i++) {
      idValues += `('${memberIds[i]}'::uuid)`

      if (i !== memberIds.length - 1) {
        idValues += ','
      }
    }

    const query = `WITH id_list (id) AS (
      VALUES
          ${idValues}
        )
        SELECT id
        FROM id_list
        WHERE NOT EXISTS (
            SELECT 1
            FROM members m
            WHERE m.id = id_list.id
        );`

    try {
      const results: IMemberId[] = await this.connection.any(query)

      return results.map((r) => r.id)
    } catch (error) {
      this.log.error('Error while getting non existing members from db', error)
      throw error
    }
  }

  async getMembers(memberIds: string[]): Promise<ILLMConsumableMemberDbResult[]> {
    try {
      const result: ILLMConsumableMemberDbResult[] = await this.connection.manyOrNone(
        `
        select
          mem.attributes,
          mem."displayName",
          mem."joinedAt",
          jsonb_agg(distinct mI) as identities,
          coalesce(
            (
              select jsonb_agg(
                jsonb_build_object(
                  'displayName', o."displayName",
                  'logo', o.logo,
                  'dateStart', mo."dateStart",
                  'dateEnd', mo."dateEnd",
                  'title', mo.title
                )
              )
              from "memberOrganizations" mo
              join organizations o on o.id = mo."organizationId"
              where mo."memberId" = mem.id
            ),
            '[]'::jsonb
          ) as organizations
        from members mem
        join "memberIdentities" mI
          on mem.id = mI."memberId" and mI."deletedAt" is null
        where mem.id in ($(memberIds:csv))
        group by mem.id, mem.attributes, mem."displayName", mem."joinedAt";`,
        {
          memberIds,
        },
      )

      return result || []
    } catch (err) {
      throw new Error(err)
    }
  }

  async getRawMemberSuggestions(
    similarityFilter: { lte: number; gte: number },
    limit: number,
  ): Promise<string[][]> {
    const replacements: IFindRawMemberMergeSuggestionsReplacement = { limit }
    let similarityLTEFilter = ''
    let similarityGTEFilter = ''

    if (similarityFilter.lte) {
      similarityLTEFilter = ` and mtmr."similarity" <= $(similarityLTEFilter)`
      replacements.similarityLTEFilter = similarityFilter.lte
    }

    if (similarityFilter.gte) {
      similarityGTEFilter = ` and mtmr."similarity" >= $(similarityGTEFilter)`
      replacements.similarityGTEFilter = similarityFilter.gte
    }

    const query = `select * from "memberToMergeRaw" mtmr
                     where
                     not exists (
                          select 1 from "llmSuggestionVerdicts" lsv
                          where (
                              lsv."primaryId" = mtmr."memberId" and
                              lsv."secondaryId" = mtmr."toMergeId" and
                              lsv.type = '${LLMSuggestionVerdictType.MEMBER}'
                            )
                              or
                            (
                              lsv."primaryId" = mtmr."toMergeId" and
                              lsv."secondaryId" = mtmr."memberId" and
                              lsv.type = '${LLMSuggestionVerdictType.MEMBER}'
                            )
                     )
                     ${similarityLTEFilter}
                     ${similarityGTEFilter}
                     order by mtmr."memberId" desc
                     limit $(limit);`

    const results: IRawMemberMergeSuggestionResult[] = await this.connection.any(
      query,
      replacements,
    )

    return results.map((r) => [r.memberId, r.toMergeId])
  }

  async removeMemberMergeSuggestion(
    suggestion: string[],
    table: MemberMergeSuggestionTable,
  ): Promise<void> {
    const query = `
      delete from "${table}"
      where
        ("memberId" = $(memberId) and "toMergeId" = $(toMergeId))
        or
        ("memberId" = $(toMergeId) and "toMergeId" = $(memberId))
    `

    const replacements = {
      memberId: suggestion[0],
      toMergeId: suggestion[1],
    }

    try {
      await this.connection.none(query, replacements)
    } catch (error) {
      this.log.error(`Error removing member suggestions from ${table}`, error)
      throw error
    }
  }
}

export default MemberMergeSuggestionsRepository
