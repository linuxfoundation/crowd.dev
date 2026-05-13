import { queryAuditLogs } from '@crowd/data-access-layer'
import { LoggerBase } from '@crowd/logging'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import { IServiceOptions } from './IServiceOptions'

export default class AuditLogsService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  public async query(query: any) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    return queryAuditLogs(qx, query)
  }
}
