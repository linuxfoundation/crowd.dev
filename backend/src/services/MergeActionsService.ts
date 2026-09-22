import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { findEntityMergeActions } from '@crowd/data-access-layer/src/mergeActions/repo'
import { LoggerBase } from '@crowd/logging'

import { IServiceOptions } from './IServiceOptions'

export default class MergeActionsService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  async query(args) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)

    const filters = {
      state: args.state,
      limit: args.limit,
      offset: args.offset,
    }

    const results = await findEntityMergeActions(qx, args.entityId, args.type, filters)

    return results.map((result) => ({
      primaryId: result.primaryId,
      secondaryId: result.secondaryId,
      state: result.state,
      // derive operation type from step and if step is null, default to merge
      operationType: result.step ? MergeActionsService.getOperationType(result.step) : 'unknown',
    }))
  }

  static getOperationType(step: string) {
    if (step.startsWith('merge')) {
      return 'merge'
    }
    if (step.startsWith('unmerge')) {
      return 'unmerge'
    }

    throw new Error(`Unrecognized merge action step: ${step}`)
  }
}
