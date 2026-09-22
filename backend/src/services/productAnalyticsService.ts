import {
  createEvent,
  createSession,
  updateSession,
} from '@crowd/data-access-layer/src/productAnalytics'
import { PgPromiseQueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { LoggerBase } from '@crowd/logging'

import { IServiceOptions } from './IServiceOptions'

export default class ProductAnalyticsService extends LoggerBase {
  private readonly qx: PgPromiseQueryExecutor

  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
    this.qx = new PgPromiseQueryExecutor(this.options.productDb)
  }

  public async createSession(data) {
    try {
      return await createSession(this.qx, data)
    } catch {
      throw new Error('Error during session create!')
    }
  }

  public async updateSession(id, data) {
    try {
      return await updateSession(this.qx, id, data)
    } catch {
      throw new Error('Error during session update!')
    }
  }

  public async createEvent(data) {
    try {
      return await createEvent(this.qx, data)
    } catch {
      throw new Error('Error during event create!')
    }
  }
}
