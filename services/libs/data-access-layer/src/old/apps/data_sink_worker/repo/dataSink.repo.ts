import { DEFAULT_TENANT_ID, distinct, singleOrDefault } from '@crowd/common'
import { DbStore, RepositoryBase } from '@crowd/database'
import { Logger } from '@crowd/logging'
import { IIntegrationResult, IntegrationResultState } from '@crowd/types'

import { IDelayedResults, IFailedResultData, IIntegrationData, IResultData } from './dataSink.data'

export default class DataSinkRepository extends RepositoryBase<DataSinkRepository> {
  constructor(dbStore: DbStore, parentLog: Logger) {
    super(dbStore, parentLog)
  }

  private readonly getResultInfoQuery = `
    select r.id,
           r.state,
           r.data, 
           r."runId",
           r."webhookId",
           r."streamId",
           r."integrationId",
           r.retries,
           r."delayedUntil",
           i.platform,
           i."segmentId",
           run.onboarding
    from integration.results r
        left join integrations i on r."integrationId" = i.id
        left join integration.runs run on run.id = r."runId"
    where 
  `
  public async getResultInfo(resultId: string): Promise<IResultData | null> {
    const result = await this.db().oneOrNone(`${this.getResultInfoQuery} r.id = $(resultId)`, {
      resultId,
    })
    return result
  }

  public async getResultInfos(resultIds: string[]): Promise<IResultData[]> {
    const results = await this.db().any(`${this.getResultInfoQuery} r.id in ($(resultIds:csv))`, {
      resultIds,
    })
    return results
  }

  public async getIntegrationInfos(integrationIds: string[]): Promise<IIntegrationData[]> {
    const results = await this.db().any(
      `select id as "integrationId",
              "segmentId",
              platform
       from integrations where id in ($(integrationIds:csv))`,
      {
        integrationIds,
      },
    )

    return results
  }

  public async getOldResultsToProcessForTenant(limit: number, lastId?: string): Promise<string[]> {
    try {
      const results = await this.db().any(
        `
        select r.id
        from integration.results r
        where (r.state = $(pendingState)
          or (r.state = $(delayedState) and r."delayedUntil" < now()))
          ${lastId !== undefined ? 'and r.id > $(lastId)' : ''}
        order by r.id
        limit ${limit};
        `,
        // ERROR rows are intentionally excluded: ERROR is a terminal state — a row reaches it
        // only after exhausting all retries or hitting an unrepeatable error. Re-queuing it
        // without resetting retries means it will immediately error again on the next attempt.
        // To retry a specific error row, reset it explicitly first (state = PENDING, retries = 0).
        {
          pendingState: IntegrationResultState.PENDING,
          delayedState: IntegrationResultState.DELAYED,
          lastId,
        },
      )

      return results.map((s) => s.id)
    } catch (err) {
      this.log.error(err, 'Failed to get old results to process!')
      throw err
    }
  }

  public async touchUpdatedAt(resultIds: string[]): Promise<void> {
    if (resultIds.length === 0) {
      return
    }

    try {
      await this.db().none(
        `
        update integration.results set "updatedAt" = now()
        where id in ($(resultIds:csv))
      `,
        {
          resultIds,
        },
      )
    } catch (err) {
      this.log.error(err, 'Failed to touch updatedAt for results!')
      throw err
    }
  }

  public async markResultError(
    resultId: string,
    error: unknown,
    resultToCreate?: IResultData,
  ): Promise<void> {
    if (resultToCreate) {
      const result = await this.db().result(
        `
      insert into integration.results(state, data, "tenantId", "integrationId", error)
      values($(state), $(data), $(tenantId), $(integrationId), $(error))
      `,
        {
          tenantId: DEFAULT_TENANT_ID,
          integrationId: resultToCreate.integrationId,
          state: IntegrationResultState.ERROR,
          data: JSON.stringify(resultToCreate.data),
          error: JSON.stringify(error),
        },
      )

      this.checkUpdateRowCount(result.rowCount, 1)
    } else {
      const result = await this.db().result(
        `update integration.results
           set state = $(state),
               "processedAt" = now(),
               error = $(error),
               "updatedAt" = now()
         where id = $(resultId)`,
        {
          resultId,
          state: IntegrationResultState.ERROR,
          error: JSON.stringify(error),
        },
      )

      this.checkUpdateRowCount(result.rowCount, 1)
    }
  }

  public async deleteResults(resultIds: string[]): Promise<void> {
    await this.db().none(`delete from integration.results where id in ($(resultIds:csv))`, {
      resultIds,
    })
  }

  public async touchRun(runId: string): Promise<void> {
    const result = await this.db().result(
      `
      update integration.runs
         set "updatedAt" = now()
       where id = $(runId)
    `,
      {
        runId,
      },
    )

    this.checkUpdateRowCount(result.rowCount, 1)
  }

  public async getFailedResults(page: number, perPage: number): Promise<IFailedResultData[]> {
    const results = await this.db().any(
      `select r.id,
              i.platform,
              run.onboarding
        from integration.results r
         inner join integrations i on i.id = r."integrationId"
         left join integration.runs run on run.id = r."runId"
        where r.state = $(state)
       order by r."createdAt" asc
       limit $(perPage) offset $(offset)`,
      {
        state: IntegrationResultState.ERROR,
        perPage,
        offset: (page - 1) * perPage,
      },
    )

    return results
  }

  public async getFailedResultsForRun(
    runId: string,
    page: number,
    perPage: number,
  ): Promise<IFailedResultData[]> {
    const results = await this.db().any(
      `select r.id,
              i.platform,
              run.onboarding
        from integration.results r
         inner join integrations i on i.id = r."integrationId"
         inner join integration.runs run on run.id = r."runId"
        where r."runId" = $(runId) and r.state = $(state)
       order by r."createdAt" asc
       limit $(perPage) offset $(offset)`,
      {
        runId,
        state: IntegrationResultState.ERROR,
        perPage,
        offset: (page - 1) * perPage,
      },
    )

    return results
  }

  public async resetResults(resultIds: string[]): Promise<void> {
    const result = await this.db().result(
      `update integration.results
        set state = $(newState),
            error = null,
            "updatedAt" = now(),
            "delayedUntil" = null
        where id in ($(resultIds:csv))`,
      {
        resultIds,
        newState: IntegrationResultState.PENDING,
      },
    )

    this.checkUpdateRowCount(result.rowCount, resultIds.length)
  }

  public async getSegmentIds(): Promise<string[]> {
    const result = await this.db().any(`select id from "segments"`)

    return result.map((r) => r.id)
  }

  public async delayResult(
    resultId: string,
    until: Date,
    error: unknown,
    resultToCreate?: IResultData,
  ): Promise<void> {
    if (resultToCreate) {
      const result = await this.db().result(
        `
          insert into integration.results(state, data, "tenantId", "integrationId", error, retries, "delayedUntil")
          values($(state), $(data), $(tenantId), $(integrationId), $(error), $(retries), $(until))
        `,
        {
          tenantId: DEFAULT_TENANT_ID,
          integrationId: resultToCreate.integrationId,
          state: IntegrationResultState.DELAYED,
          data: JSON.stringify(resultToCreate.data),
          retries: 1,
          error: JSON.stringify(error),
          until: until,
        },
      )
      this.checkUpdateRowCount(result.rowCount, 1)
    } else {
      const result = await this.db().result(
        `update integration.results
         set  state = $(state),
              error = $(error),
              "delayedUntil" = $(until),
              retries = coalesce(retries, 0) + 1,
              "updatedAt" = now()
         where id = $(resultId)`,
        {
          resultId,
          until,
          error: JSON.stringify(error),
          state: IntegrationResultState.DELAYED,
        },
      )

      this.checkUpdateRowCount(result.rowCount, 1)
    }
  }

  public async publishExternalResult(
    id: string,
    integrationId: string,
    result: IIntegrationResult,
  ): Promise<void> {
    await this.db().none(
      `
      insert into integration.results(id, state, data, "tenantId", "integrationId")
      values($(id), $(state), $(data)::json, $(tenantId), $(integrationId))
      `,
      {
        id,
        state: IntegrationResultState.PENDING,
        tenantId: DEFAULT_TENANT_ID,
        data: JSON.stringify(result),
        integrationId,
      },
    )
  }

  public async getDelayedResults(limit: number): Promise<IDelayedResults[]> {
    this.ensureTransactional()

    try {
      const resultData = await this.db().any(
        `
        select r.id,
               r."runId"
        from integration.results r
        where r.state = $(delayedState) and r."delayedUntil" < now()
        limit ${limit}
        for update skip locked;
        `,
        {
          delayedState: IntegrationResultState.DELAYED,
        },
      )

      const runIds = distinct(resultData.map((r) => r.runId))
      if (runIds.length > 0) {
        const runInfos = await this.db().any(
          `
        select id, onboarding
        from integration.runs
        where id in ($(runIds:csv))
        `,
          {
            runIds,
          },
        )

        for (const result of resultData) {
          const runInfo = singleOrDefault(runInfos, (r) => r.id === result.runId)
          if (runInfo) {
            result.onboarding = runInfo.onboarding
          } else {
            result.onboarding = true
          }
        }
      }

      return resultData
    } catch (err) {
      this.log.error(err, 'Failed to get delayed results!')
      throw err
    }
  }
}
