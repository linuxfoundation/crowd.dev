import { Logger } from '@crowd/logging'

import { IRepositoryOptions } from '../../database/repositories/IRepositoryOptions'
import { IServiceOptions } from '../../services/IServiceOptions'

export interface IPendingStream {
  value: string
  metadata?: any
}

export interface IIntegrationStream extends IPendingStream {
  id: string
}

export interface IFailedIntegrationStream extends IIntegrationStream {
  retries: number
}

export interface IStreamResultOperation {
  type: string
  records: any[]
}

export interface IProcessStreamResults {
  // result of stream processing are operations that have to be done after
  operations: IStreamResultOperation[]

  // which was the last record that was processed in the current stream
  lastRecord?: any

  // last record timestamp in the current stream
  lastRecordTimestamp?: number

  // if processing of the current stream results in new streams they should be returned here
  newStreams?: IPendingStream[]

  // if processing of the current stream results in the next page of the same stream it should be returned here
  nextPageStream?: IPendingStream

  // seconds to pause between continuing with integration processing for the remaining streams
  sleep?: number
}

export interface IProcessWebhookResults {
  // result of stream processing are operations that have to be done after
  operations: IStreamResultOperation[]
  // seconds to pause between continuing with integration processing for the remaining streams
  sleep?: number
}

export interface IStepContext {
  // when did integration processing start
  startTimestamp: number

  // are we doing onboarding - the first run of the integration?
  onboarding: boolean

  // data that an individual integration service can use across the whole integration processing pipeline
  pipelineData: any

  runId?: string

  // integration that we are currently processing
  integration?: any

  // webhook that we are currently processing
  webhook?: any

  // repository options for integration services to use when creating new instances of repositories
  repoContext: IRepositoryOptions

  // service options for integration services to use when creating new instances of services
  serviceContext: IServiceOptions

  // logger associated with the integration
  logger: Logger
}
