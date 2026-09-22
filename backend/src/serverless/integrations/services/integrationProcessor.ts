import { LoggerBase } from '@crowd/logging'

import IntegrationRunRepository from '../../../database/repositories/integrationRunRepository'
import { IServiceOptions } from '../../../services/IServiceOptions'
import { IntegrationTickProcessor } from './integrationTickProcessor'

export class IntegrationProcessor extends LoggerBase {
  private readonly tickProcessor: IntegrationTickProcessor

  constructor(options: IServiceOptions) {
    super(options.log)

    const integrationRunRepository = new IntegrationRunRepository(options)

    this.tickProcessor = new IntegrationTickProcessor(options, integrationRunRepository)
  }

  async processTick() {
    await this.tickProcessor.processTick()
  }
}
