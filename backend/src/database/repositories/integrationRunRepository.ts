import { QueryTypes } from 'sequelize'

import { generateUUIDv1 } from '@crowd/common'
import { IntegrationRunState } from '@crowd/types'

import { INTEGRATION_PROCESSING_CONFIG } from '../../conf'
import { DbIntegrationRunCreateData, IntegrationRun } from '../../types/integrationRunTypes'
import { IntegrationStreamState } from '../../types/integrationStreamTypes'
import { IRepositoryOptions } from './IRepositoryOptions'
import { RepositoryBase } from './repositoryBase'

export default class IntegrationRunRepository extends RepositoryBase<
  IntegrationRun,
  string,
  DbIntegrationRunCreateData,
  unknown,
  unknown
> {
  public constructor(options: IRepositoryOptions) {
    super(options, true)
  }

  async findDelayedRuns(page: number, perPage: number): Promise<IntegrationRun[]> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
      from "integrationRuns"
      where state = :delayedState and "delayedUntil" <= now()
      order by "createdAt" desc
      limit ${perPage} offset ${(page - 1) * perPage}
    `

    const results = await seq.query(query, {
      replacements: {
        delayedState: IntegrationRunState.DELAYED,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    return results as IntegrationRun[]
  }

  async findIntegrationsByState(
    states: IntegrationRunState[],
    page: number,
    perPage: number,
    lastCreatedAt?: string,
  ): Promise<IntegrationRun[]> {
    const seq = this.seq

    const replacements: any = {}

    const stateParams: string[] = states.map((state, index) => {
      replacements[`state${index}`] = state
      return `:state${index}`
    })

    const conditions = []
    if (lastCreatedAt) {
      replacements.lastCreatedAt = lastCreatedAt
      conditions.push('"createdAt" < :lastCreatedAt')
    }

    let conditionString = ''
    if (conditions.length > 0) {
      conditionString = ` and ${conditions.join(' and ')}`
    }

    const query = `
      select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
      from "integrationRuns"
      where state in (${stateParams.join(', ')}) ${conditionString}
      order by "createdAt" desc
      limit ${perPage} offset ${(page - 1) * perPage}
    `

    const results = await seq.query(query, {
      replacements,
      type: QueryTypes.SELECT,
    })

    return results as IntegrationRun[]
  }

  async findLastRun(integrationId: string): Promise<IntegrationRun | undefined> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
    select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
    from "integrationRuns"
    where "integrationId" = :integrationId
    order by "createdAt" desc
    limit 1
    `

    const results = await seq.query(query, {
      replacements: {
        integrationId,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    if (results.length === 0) {
      return undefined
    }

    return results[0] as IntegrationRun
  }

  async findStuckIntegrationRuns(
    integrationId: string,
    hoursThreshold: number = 24,
  ): Promise<IntegrationRun[]> {
    const transaction = this.transaction
    const seq = this.seq
    const replacements: any = {
      delayedState: IntegrationRunState.DELAYED,
      processingState: IntegrationRunState.PROCESSING,
      pendingState: IntegrationRunState.PENDING,
      integrationId,
    }
    const runsWithStuckStreamsQuery = `
    SELECT DISTINCT run.id, run.onboarding
    FROM integration.runs run
    JOIN integration.streams strm ON run."id" = strm."runId"
    WHERE 
      run.state IN (:delayedState, :processingState, :pendingState)
    AND strm.state IN (:delayedState, :pendingState)
    AND run."integrationId" = :integrationId
    AND run."createdAt" < NOW() - INTERVAL '${hoursThreshold} hours'
    `
    const results = await seq.query(runsWithStuckStreamsQuery, {
      replacements,
      type: QueryTypes.SELECT,
      transaction,
    })

    return results as IntegrationRun[]
  }

  async cleanupOrphanedIntegrationRuns(
    integrationId: string,
    hoursThreshold: number = 24,
  ): Promise<void> {
    const transaction = this.transaction
    const seq = this.seq
    const replacements: any = {
      integrationId,
      delayedState: IntegrationRunState.DELAYED,
      processingState: IntegrationRunState.PROCESSING,
      pendingState: IntegrationRunState.PENDING,
    }

    const query = `
    DELETE FROM integration.runs run
    WHERE NOT EXISTS (
        SELECT 1 
        FROM integration.streams strm 
        WHERE strm."runId" = run."id"
    )
    AND run."integrationId" = :integrationId
    AND run."createdAt" < NOW() - INTERVAL '${hoursThreshold} hours'
    AND run.state IN (:delayedState, :processingState, :pendingState);
    `

    await seq.query(query, {
      replacements,
      type: QueryTypes.DELETE,
      transaction,
    })
  }

  async findLastProcessingRunInNewFramework(integrationId: string): Promise<string> {
    const transaction = this.transaction

    const seq = this.seq

    const condition = ` "integrationId" = :integrationId `

    const replacements: any = {
      delayedState: IntegrationRunState.DELAYED,
      processingState: IntegrationRunState.PROCESSING,
      pendingState: IntegrationRunState.PENDING,
      integrationId,
    }

    const query = `
    select id
    from integration.runs
    where state in (:delayedState, :processingState, :pendingState) and ${condition}
    order by "createdAt" desc
    limit 1
    `

    const results = await seq.query(query, {
      replacements,
      type: QueryTypes.SELECT,
      transaction,
    })

    if (results.length === 1) {
      return (results[0] as any).id
    }

    return undefined
  }

  async findLastProcessingRun(
    integrationId?: string,
    ignoreId?: string,
  ): Promise<IntegrationRun | undefined> {
    const transaction = this.transaction

    const seq = this.seq

    let condition = ``
    const replacements: any = {
      delayedState: IntegrationRunState.DELAYED,
      processingState: IntegrationRunState.PROCESSING,
      pendingState: IntegrationRunState.PENDING,
    }

    if (integrationId) {
      condition = ` "integrationId" = :integrationId `
      replacements.integrationId = integrationId
    } else {
      throw new Error(`integrationId must be provided!`)
    }

    if (ignoreId) {
      condition = `${condition} and id <> :ignoreId`
      replacements.ignoreId = ignoreId
    }

    const query = `
    select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
    from "integrationRuns"
    where state in (:delayedState, :processingState, :pendingState) and ${condition}
    order by "createdAt" desc
    limit 1
    `

    const results = await seq.query(query, {
      replacements,
      type: QueryTypes.SELECT,
      transaction,
    })

    if (results.length === 1) {
      return results[0] as IntegrationRun
    }

    return undefined
  }

  override async findById(id: string): Promise<IntegrationRun> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
    select id,
           "tenantId",
          "integrationId",
          onboarding,
          state,
          "delayedUntil",
          "processedAt",
          error,
          "createdAt",
          "updatedAt"
      from "integrationRuns" where id = :id;      
    `

    const result = await seq.query(query, {
      replacements: {
        id,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    if (result.length !== 1) {
      throw new Error(`Expected 1 row to be selected, got ${result.length} rows instead.`)
    }

    return result[0] as IntegrationRun
  }

  override async create(data: DbIntegrationRunCreateData): Promise<IntegrationRun> {
    const transaction = this.transaction

    const seq = this.seq

    const id = generateUUIDv1()

    const query = `
      insert into "integrationRuns"(id, "tenantId", "integrationId", onboarding, state)
      values(:id, :tenantId, :integrationId, :onboarding, :state)
      returning "createdAt";
    `

    const result = await seq.query(query, {
      replacements: {
        id,
        tenantId: data.tenantId,
        integrationId: data.integrationId || null,
        onboarding: data.onboarding,
        state: data.state,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    if (result.length !== 1) {
      throw new Error(`Expected 1 row to be inserted, got ${result.length} rows instead.`)
    }

    return {
      id,
      tenantId: data.tenantId,
      integrationId: data.integrationId,
      onboarding: data.onboarding,
      state: data.state,
      delayedUntil: null,
      processedAt: null,
      error: null,
      createdAt: (result[0] as any).createdAt,
      updatedAt: (result[0] as any).createdAt,
    }
  }

  async markProcessing(id: string): Promise<void> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      update "integrationRuns"
      set state = :state,
          "delayedUntil" = null,
          "updatedAt" = now()
      where id = :id
    `

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, rowCount] = await seq.query(query, {
      replacements: {
        id,
        state: IntegrationRunState.PROCESSING,
      },
      type: QueryTypes.UPDATE,
      transaction,
    })

    if (rowCount !== 1) {
      throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`)
    }
  }

  async restart(id: string): Promise<void> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      update "integrationRuns"
        set state = :state,
            "delayedUntil" = null,
            "processedAt" = null,
            error = null,
            "updatedAt" = now()
      where id = :id
    `

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, rowCount] = await seq.query(query, {
      replacements: {
        id,
        state: IntegrationRunState.PENDING,
      },
      type: QueryTypes.UPDATE,
      transaction,
    })

    if (rowCount !== 1) {
      throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`)
    }
  }

  async markError(id: string, error: any): Promise<void> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      update "integrationRuns"
      set state = :state,
          error = :error,
          "updatedAt" = now()
      where id = :id
    `

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, rowCount] = await seq.query(query, {
      replacements: {
        id,
        state: IntegrationRunState.ERROR,
        error: JSON.stringify(error),
      },
      type: QueryTypes.UPDATE,
      transaction,
    })

    if (rowCount !== 1) {
      throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`)
    }
  }

  async delay(id: string, until: Date): Promise<void> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      update "integrationRuns"
      set "delayedUntil" = :until,
          state = :state,
          "updatedAt" = now()
      where id = :id
    `

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, rowCount] = await seq.query(query, {
      replacements: {
        id,
        until,
        state: IntegrationRunState.DELAYED,
      },
      type: QueryTypes.UPDATE,
      transaction,
    })

    if (rowCount !== 1) {
      throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`)
    }
  }

  async touch(id: string): Promise<void> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      update "integrationRuns"
      set "updatedAt" = now()
      where id = :id
    `

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, rowCount] = await seq.query(query, {
      replacements: {
        id,
      },
      type: QueryTypes.UPDATE,
      transaction,
    })

    if (rowCount !== 1) {
      throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`)
    }
  }

  async touchState(id: string): Promise<IntegrationRunState> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
    update "integrationRuns"
    set "processedAt" = case
                            when (select count(s.id) =
                                        (count(s.id) filter ( where s.state = :successStreamState ) +
                                          count(s.id)
                                          filter (where s.state = :errorStreamState and s.retries >= :maxRetries))
                                  from "integrationStreams" s
                                  where s."runId" = :id) then now()
        end,
        state         = case
                            when (select (count(s.id) =
                                          (count(s.id) filter ( where s.state = :successStreamState ) +
                                          count(s.id) filter (where s.state = :errorStreamState))) and
                                        (count(s.id)
                                          filter (where s.state = :errorStreamState and s.retries < :maxRetries)) = 0
                                  from "integrationStreams" s
                                  where s."runId" = :id) then :successRunState
                            when (select (count(s.id) =
                                          (count(s.id) filter ( where s.state = :successStreamState ) +
                                          count(s.id) filter (where s.state = :errorStreamState))) and
                                        (count(s.id)
                                          filter (where s.state = :errorStreamState and s.retries >= :maxRetries)) > 0
                                  from "integrationStreams" s
                                  where s."runId" = :id) then :errorRunState
                            else state
            end,
        "updatedAt"   = now()
    where id = :id
    returning state;
    `

    const result = await seq.query(query, {
      replacements: {
        id,
        successStreamState: IntegrationStreamState.PROCESSED,
        errorStreamState: IntegrationStreamState.ERROR,
        successRunState: IntegrationRunState.PROCESSED,
        errorRunState: IntegrationRunState.ERROR,
        maxRetries: INTEGRATION_PROCESSING_CONFIG.maxRetries,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    if (result.length !== 1) {
      throw new Error(`Expected 1 row to be updated, got ${result.length} rows instead.`)
    }

    return (result[0] as any).state
  }

  async cleanupOldRuns(months: number): Promise<void> {
    const seq = this.seq

    const cleanQuery = `
        delete from "integrationRuns" where state = :processed and "processedAt" < now() - interval '${months} months';                     
    `

    await seq.query(cleanQuery, {
      replacements: {
        processed: IntegrationRunState.PROCESSED,
      },
      type: QueryTypes.DELETE,
    })
  }
}
