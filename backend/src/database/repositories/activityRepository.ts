import { QueryTypes } from 'sequelize'

import { IIntegrationResult, IntegrationResultState } from '@crowd/types'

import { IRepositoryOptions } from './IRepositoryOptions'
import SequelizeRepository from './sequelizeRepository'

class ActivityRepository {
  static async createResults(result: IIntegrationResult, options: IRepositoryOptions) {
    const tenant = SequelizeRepository.getCurrentTenant(options)

    const segment = SequelizeRepository.getStrictlySingleActiveSegment(options)

    const seq = SequelizeRepository.getSequelize(options)

    result.segmentId = segment.id

    const results = await seq.query(
      `
      insert into integration.results(state, data, "tenantId")
      values(:state, :data, :tenantId)
      returning id;
      `,
      {
        replacements: {
          tenantId: tenant.id,
          state: IntegrationResultState.PENDING,
          data: JSON.stringify(result),
        },
        type: QueryTypes.INSERT,
      },
    )

    return results[0][0].id
  }
}

export default ActivityRepository
