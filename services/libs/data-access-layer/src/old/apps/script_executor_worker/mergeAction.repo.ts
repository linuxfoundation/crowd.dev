import { DbConnection, DbTransaction } from '@crowd/database'
import { Logger } from '@crowd/logging'
import { IMergeAction } from '@crowd/types'

import { EntityType, IFindMemberMergeActionReplacement } from './types'

class MergeActionRepository {
  constructor(
    private readonly connection: DbConnection | DbTransaction,
    private readonly log: Logger,
  ) {}

  async findMemberMergeActions(
    memberId: string,
    startDate?: string,
    endDate?: string,
    userId?: string,
    limit?: number,
  ) {
    let rows: IMergeAction[] = []
    let query = `
        select
          ma.id,
          ma.type,
          ma."primaryId",
          ma."secondaryId",
          ma."createdAt",
          ma."updatedAt",
          ma.step,
          ma.state
        from "mergeActions" ma
        where 
          ma."state" = 'merged' and 
          ma."primaryId" = $(memberId) and 
          ma."unmergeBackup" is not null `

    const replacements: IFindMemberMergeActionReplacement = {
      memberId,
    }

    if (startDate) {
      query += ' and ma."createdAt" > $(startDate)'
      replacements.startDate = startDate
    }

    if (endDate) {
      query += ' and ma."createdAt" < $(endDate)'
      replacements.endDate = endDate
    }

    if (userId) {
      query += ' and ma."actionBy" = $(userId)'
      replacements.userId = userId
    }

    query += ' order by ma."createdAt" desc'

    if (limit) {
      query += ' limit $(limit)'
      replacements.limit = limit
    }

    try {
      rows = await this.connection.query(query, replacements)
    } catch (err) {
      this.log.error('Error while finding merge actions!', err)

      throw new Error(err)
    }

    return rows
  }

  async findMergeActionUnmergeBackup(mergeActionId: string) {
    const rows = await this.connection.query(
      `
      select ma."unmergeBackup"
      from "mergeActions" ma
      where ma.id = $(mergeActionId)
        and ma."unmergeBackup" is not null
      `,
      { mergeActionId },
    )

    return rows[0]?.unmergeBackup ?? null
  }

  async findMergeActionsWithDeletedSecondaryEntities(
    limit: number,
    offset: number,
    entityType: EntityType,
  ): Promise<IMergeAction[]> {
    const entityConfig = {
      [EntityType.MEMBER]: { table: 'members', type: 'member' },
      [EntityType.ORGANIZATION]: { table: 'organizations', type: 'org' },
    }

    const { table, type } = entityConfig[entityType]

    return this.connection.query(
      `
      SELECT 
        ma."primaryId",
        ma."secondaryId"
      FROM "mergeActions" ma
      WHERE ma."state" = 'merged' 
        AND ma."type" = $(type)
        AND NOT EXISTS (
          SELECT 1 FROM "${table}" t 
          WHERE t.id = ma."secondaryId"
        )
      LIMIT $(limit) OFFSET $(offset)
      `,
      {
        limit,
        offset,
        type,
      },
    )
  }

  async getUnprocessedLLMApprovedSuggestions(
    batchSize: number,
    type: EntityType,
  ): Promise<{ primaryId: string; secondaryId: string }[]> {
    return this.connection.query(
      `
      select l."primaryId", l."secondaryId"
      from "llmSuggestionVerdicts" l
      left join "mergeActions" ma on l."primaryId" = ma."primaryId" and l."secondaryId" = ma."secondaryId"
      where l."type" = $(type) and l.verdict = 'true'
      and (ma."primaryId" is null and ma."secondaryId" is null)
      ${type === EntityType.MEMBER ? 'and exists (select 1 from members m1 where m1.id = l."primaryId") and exists (select 1 from members m2 where m2.id = l."secondaryId")' : ''}
      ${type === EntityType.ORGANIZATION ? 'and exists (select 1 from organizations o1 where o1.id = l."primaryId") and exists (select 1 from organizations o2 where o2.id = l."secondaryId") and not exists (select 1 from "lfxMemberships" lm where lm."organizationId" = l."secondaryId")' : ''}
      limit $(batchSize)
    `,
      {
        type,
        batchSize,
      },
    )
  }
}

export default MergeActionRepository
