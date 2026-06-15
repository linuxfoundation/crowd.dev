import { DbConnection, DbTransaction } from '@crowd/database'
import { Logger } from '@crowd/logging'
import {
  IOrganizationMergeSuggestion,
  LLMSuggestionVerdictType,
  OrganizationMergeSuggestionTable,
  SuggestionType,
} from '@crowd/types'

import {
  IFindRawOrganizationMergeSuggestionsReplacement,
  IOrganizationId,
  IOrganizationMergeSuggestionsLatestGeneratedAt,
  IOrganizationNoMerge,
  IRawOrganizationMergeSuggestionResult,
} from './types'
import { chunkArray, removeDuplicateSuggestions } from './utils'

class OrganizationMergeSuggestionsRepository {
  constructor(
    private readonly connection: DbConnection | DbTransaction,
    private readonly log: Logger,
  ) {}

  async findNoMergeIds(organizationId: string): Promise<string[]> {
    try {
      const results: IOrganizationNoMerge[] = await this.connection.any(
        `select onm."organizationId", onm."noMergeId" from "organizationNoMerge" onm
        where onm."organizationId" = $(id) or onm."noMergeId" = $(id);`,
        {
          id: organizationId,
        },
      )
      return Array.from(
        results.reduce((acc, r) => {
          if (organizationId === r.organizationId) {
            acc.add(r.noMergeId)
          } else if (organizationId === r.noMergeId) {
            acc.add(r.organizationId)
          }
          return acc
        }, new Set<string>()),
      )
    } catch (err) {
      throw new Error(err)
    }
  }

  async findTenantsLatestOrganizationSuggestionGeneratedAt(tenantId: string): Promise<string> {
    try {
      const result: IOrganizationMergeSuggestionsLatestGeneratedAt =
        await this.connection.oneOrNone(
          `
        select "organizationMergeSuggestionsLastGeneratedAt"
        from tenants
        where "id" = $(tenantId);`,
          {
            tenantId,
          },
        )
      return result?.organizationMergeSuggestionsLastGeneratedAt
    } catch (err) {
      throw new Error(err)
    }
  }

  async updateOrganizationMergeSuggestionsLastGeneratedAt(tenantId: string): Promise<void> {
    try {
      await this.connection.any(
        `
          update tenants set "organizationMergeSuggestionsLastGeneratedAt" = now()
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
    suggestions: IOrganizationMergeSuggestion[],
    table: OrganizationMergeSuggestionTable,
  ): Promise<void> {
    // Remove possible duplicates
    suggestions = removeDuplicateSuggestions<IOrganizationMergeSuggestion>(
      suggestions,
      SuggestionType.ORGANIZATIONS,
    )

    // check all suggestion ids exists in the db
    const uniqueOrganizationIds = Array.from(
      suggestions.reduce((acc, suggestion) => {
        acc.add(suggestion.organizations[0])
        acc.add(suggestion.organizations[1])
        return acc
      }, new Set<string>()),
    )

    // filter non existing organization ids from suggestions
    const nonExistingIds = await this.findNonExistingIds(uniqueOrganizationIds)

    suggestions = suggestions.filter(
      (s) =>
        !nonExistingIds.includes(s.organizations[0]) &&
        !nonExistingIds.includes(s.organizations[1]),
    )

    // Process suggestions in chunks of 100 or less
    const suggestionChunks: IOrganizationMergeSuggestion[][] =
      chunkArray<IOrganizationMergeSuggestion>(suggestions, 100)

    const insertValues = (
      organizationId: string,
      toMergeId: string,
      similarity: number | null,
      index: number,
    ) => {
      const idPlaceholder = (key: string) => `${key}${index}`
      return {
        query: `($(${idPlaceholder('organizationId')})::uuid, $(${idPlaceholder(
          'toMergeId',
        )})::uuid, $(${idPlaceholder('similarity')}), NOW(), NOW())`,
        replacements: {
          [idPlaceholder('organizationId')]: organizationId,
          [idPlaceholder('toMergeId')]: toMergeId,
          [idPlaceholder('similarity')]: similarity,
        },
      }
    }

    for (const suggestionChunk of suggestionChunks) {
      const placeholders: string[] = []
      let replacements: Record<string, unknown> = {}

      suggestionChunk.forEach((suggestion, index) => {
        const { query, replacements: chunkReplacements } = insertValues(
          suggestion.organizations[0],
          suggestion.organizations[1],
          suggestion.similarity,
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
              "updatedAt" = now()
            from (
              values
                ${placeholders.join(', ')}
            ) as v("organizationId", "toMergeId", "similarity", "createdAt", "updatedAt")
            where
              (t."organizationId" = v."organizationId"::uuid and t."toMergeId" = v."toMergeId"::uuid)
              or
              (t."organizationId" = v."toMergeId"::uuid and t."toMergeId" = v."organizationId"::uuid);
          `,
          replacements,
        )

        // 2. insert only new rows and enforce bidirectional uniqueness
        await this.connection.none(
          `
            insert into "${table}"
              ("organizationId", "toMergeId", "similarity", "createdAt", "updatedAt")
            select v.*
            from (
              values
                ${placeholders.join(', ')}
            ) as v("organizationId", "toMergeId", "similarity", "createdAt", "updatedAt")
            where not exists (
              select 1
              from "${table}" t
              where
                (t."organizationId" = v."organizationId"::uuid and t."toMergeId" = v."toMergeId"::uuid)
                or
                (t."organizationId" = v."toMergeId"::uuid and t."toMergeId" = v."organizationId"::uuid)
            );
          `,
          replacements,
        )
      } catch (error) {
        this.log.error('Error adding organizations to merge', error)
        throw error
      }
    }
  }

  async findNonExistingIds(organizationIds: string[]): Promise<string[]> {
    let idValues = ``

    for (let i = 0; i < organizationIds.length; i++) {
      idValues += `('${organizationIds[i]}'::uuid)`

      if (i !== organizationIds.length - 1) {
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
            FROM organizations o
            WHERE o.id = id_list.id
        );`

    try {
      const results: IOrganizationId[] = await this.connection.any(query)

      return results.map((r) => r.id)
    } catch (error) {
      this.log.error('Error while getting non existing organizations from db', error)
      throw error
    }
  }

  /**
   * We get raw (unfiltered) suggestions from the database.
   * When onlyLFXMembers is true it only returns suggestions for lfx member organizations.
   * All returned suggestions are checked against the "llmSuggestionVerdicts" table to see if they have already been processed.
   * Already processed suggestions will not be returned.
   * @param similarityFilter
   * @param limit
   * @param onlyLFXMembers
   * @returns
   */
  async getRawOrganizationSuggestions(
    similarityFilter: { lte: number; gte: number },
    limit: number,
    onlyLFXMembers = false,
    organizationIds = [],
  ): Promise<string[][]> {
    const replacements: IFindRawOrganizationMergeSuggestionsReplacement = { limit }

    let query: string
    let organizationIdFilter = ''
    let similarityLTEFilter = ''
    let similarityGTEFilter = ''

    if (similarityFilter && similarityFilter.lte) {
      similarityLTEFilter = ` and otmr."similarity" <= $(similarityLTEFilter)`
      replacements.similarityLTEFilter = similarityFilter.lte
    }

    if (similarityFilter && similarityFilter.gte) {
      similarityGTEFilter = ` and otmr."similarity" >= $(similarityGTEFilter)`
      replacements.similarityGTEFilter = similarityFilter.gte
    }

    if (organizationIds.length > 0) {
      organizationIdFilter = ` and (otmr."organizationId" in ($(organizationIds:csv)) or otmr."toMergeId" in ($(organizationIds:csv)))`
      replacements.organizationIds = organizationIds
    }
    if (onlyLFXMembers) {
      query = `with suggestions as (
                        select otmr."organizationId", otmr."toMergeId" from "organizationToMergeRaw" otmr
                        where exists(
                            select 1 from "lfxMemberships" lfm
                            where otmr."organizationId" = lfm."organizationId" or
                                  otmr."toMergeId" = lfm."organizationId"
                        )
                        ${similarityLTEFilter}
                        ${similarityGTEFilter}
                        ${organizationIdFilter}
                     )
                     select distinct s."organizationId", s."toMergeId"
                     from suggestions s
                     where not exists (
                          select 1 from "llmSuggestionVerdicts" lsv
                          where (
                              lsv."primaryId" = s."organizationId" and
                              lsv."secondaryId" = s."toMergeId" and
                              lsv.type = '${LLMSuggestionVerdictType.ORGANIZATION}'
                            )
                              or
                            (
                              lsv."primaryId" = s."toMergeId" and
                              lsv."secondaryId" = s."organizationId" and
                              lsv.type = '${LLMSuggestionVerdictType.ORGANIZATION}'

                            )
                     )
                     limit $(limit);`
    } else {
      query = `select * from "organizationToMergeRaw" otmr
                     where
                     not exists (
                          select 1 from "llmSuggestionVerdicts" lsv
                          where (
                              lsv."primaryId" = otmr."organizationId" and
                              lsv."secondaryId" = otmr."toMergeId" and
                              lsv.type = '${LLMSuggestionVerdictType.ORGANIZATION}'
                            )
                              or
                            (
                              lsv."primaryId" = otmr."toMergeId" and
                              lsv."secondaryId" = otmr."organizationId" and
                              lsv.type = '${LLMSuggestionVerdictType.ORGANIZATION}'
                            )
                     )
                     ${similarityLTEFilter}
                     ${similarityGTEFilter}
                     limit $(limit);`
    }

    const results: IRawOrganizationMergeSuggestionResult[] = await this.connection.any(
      query,
      replacements,
    )

    return results.map((r) => [r.organizationId, r.toMergeId])
  }

  async removeOrganizationMergeSuggestions(
    suggestion: string[],
    table: OrganizationMergeSuggestionTable,
  ): Promise<void> {
    const query = `
      delete from "${table}" 
      where 
        ("organizationId" = $(organizationId) and "toMergeId" = $(toMergeId))
        or 
        ("organizationId" = $(toMergeId) and "toMergeId" = $(organizationId))
    `

    const replacements = {
      organizationId: suggestion[0],
      toMergeId: suggestion[1],
    }

    try {
      await this.connection.none(query, replacements)
    } catch (error) {
      this.log.error(`Error removing organization suggestions rom ${table}!`, error)
      throw error
    }
  }
}

export default OrganizationMergeSuggestionsRepository
