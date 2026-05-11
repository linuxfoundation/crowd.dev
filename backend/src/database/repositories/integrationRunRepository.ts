import { QueryTypes } from 'sequelize'

import { IntegrationRunState, SearchCriteria } from '@crowd/types'

import { IntegrationRun } from '../../types/integrationRunTypes'

import { IRepositoryOptions } from './IRepositoryOptions'
import { RepositoryBase } from './repositoryBase'

export default class IntegrationRunRepository extends RepositoryBase<
  IntegrationRun,
  string,
  SearchCriteria,
  SearchCriteria,
  SearchCriteria
> {
  public constructor(options: IRepositoryOptions) {
    super(options, true)
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
    const query = `
    SELECT DISTINCT run.id, run.onboarding
    FROM integration.runs run
    JOIN integration.streams strm ON run."id" = strm."runId"
    WHERE
      run.state IN (:delayedState, :processingState, :pendingState)
    AND strm.state IN (:delayedState, :pendingState)
    AND run."integrationId" = :integrationId
    AND run."createdAt" < NOW() - INTERVAL '${hoursThreshold} hours'
    `
    const results = await seq.query(query, {
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

  async findLastProcessingRunInNewFramework(integrationId: string): Promise<string | undefined> {
    const transaction = this.transaction
    const seq = this.seq

    const replacements: any = {
      delayedState: IntegrationRunState.DELAYED,
      processingState: IntegrationRunState.PROCESSING,
      pendingState: IntegrationRunState.PENDING,
      integrationId,
    }

    const query = `
    select id
    from integration.runs
    where state in (:delayedState, :processingState, :pendingState) and "integrationId" = :integrationId
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
}
