import { Logger } from '@crowd/logging'
import {
  IActivityData,
  ICache,
  IConcurrentRequestLimiter,
  IIntegration,
  IIntegrationStream,
  IMemberAttribute,
  IRateLimiter,
  IntegrationResultType,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

export interface IIntegrationContext {
  onboarding?: boolean
  integration: IIntegration
  log: Logger
  /**
   * Cache that is tied up to the tenantId and integration type
   */
  cache: ICache

  publishStream: <T>(identifier: string, metadata?: T) => Promise<void>
  updateIntegrationSettings: (settings: unknown) => Promise<void>
  updateIntegrationToken: (token: string) => Promise<void>
  updateIntegrationRefreshToken: (refreshToken: string) => Promise<void>

  abortRunWithError: (message: string, metadata?: unknown, error?: Error) => Promise<void>
}

export interface IIntegrationProcessRemoteSyncContext {
  tenantId: string
  integration: IIntegration
  log: Logger
  serviceSettings: IIntegrationServiceSettings
}

export interface IGenerateStreamsContext extends IIntegrationContext {
  serviceSettings: IIntegrationServiceSettings
  platformSettings?: unknown
  isManualRun?: boolean
  manualSettings?: unknown
  additionalInfo?: unknown
}

export interface IProcessStreamContext extends IIntegrationContext {
  stream: IIntegrationStream
  serviceSettings: IIntegrationServiceSettings
  platformSettings?: unknown

  setMessageVisibilityTimeout: (newTimeout: number) => Promise<void>

  processData: <T>(data: T) => Promise<void>

  abortWithError: (message: string, metadata?: unknown, error?: Error) => Promise<void>

  fetchIntegrationMembersPaginated: (
    platform: PlatformType,
    type: MemberIdentityType,
    page: number,
    perPage: number,
  ) => Promise<{ id: string; value: string }[]>

  /**
   * Global cache that is shared between all integrations
   */
  globalCache: ICache

  /**
   * Cache that is shared between all streams of the same integration (integrationId)
   */
  integrationCache: ICache

  getRateLimiter: (maxRequests: number, timeWindowSeconds: number, cacheKey: string) => IRateLimiter
  getConcurrentRequestLimiter: (
    maxConcurrentRequests: number,
    cacheKey: string,
  ) => IConcurrentRequestLimiter
}

export interface IProcessWebhookStreamContext {
  integration: IIntegration
  log: Logger
  cache: ICache

  publishStream: <T>(identifier: string, metadata?: T) => Promise<void>
  updateIntegrationSettings: (settings: unknown) => Promise<void>
  stream: IIntegrationStream
  serviceSettings: IIntegrationServiceSettings
  platformSettings?: unknown

  processData: <T>(data: T) => Promise<void>

  abortWithError: (message: string, metadata?: unknown, error?: Error) => Promise<void>

  /**
   * Global cache that is shared between all integrations
   */
  globalCache: ICache

  /**
   * Cache that is shared between all streams of the same integration (integrationId)
   */
  integrationCache: ICache

  getRateLimiter: (maxRequests: number, timeWindowSeconds: number, cacheKey: string) => IRateLimiter
  getConcurrentRequestLimiter: (
    maxConcurrentRequests: number,
    cacheKey: string,
  ) => IConcurrentRequestLimiter
}

export interface IProcessDataContext extends IIntegrationContext {
  data: unknown
  platformSettings?: unknown

  abortWithError: (message: string, metadata?: unknown, error?: Error) => Promise<void>

  publishActivity: (activity: IActivityData) => Promise<void>

  publishCustom: (entity: unknown, type: IntegrationResultType) => Promise<void>
}

export type GenerateStreamsHandler = (ctx: IGenerateStreamsContext) => Promise<void>
export type ProcessStreamHandler = (ctx: IProcessStreamContext) => Promise<void>
export type ProcessWebhookStreamHandler = (ctx: IProcessWebhookStreamContext) => Promise<void>
export type ProcessDataHandler = (ctx: IProcessDataContext) => Promise<void>

export interface IIntegrationDescriptor {
  /**
   * Function that will be called to generate initial streamsfor the integration run.
   * Ideally this function should not call any external APIs and generate just initial streams
   * from which more streams can be generated later via processStream function.
   *
   * Use ctx.publishStream to create a new stream
   * @param ctx Everything that is needed to generate streams
   */
  generateStreams: GenerateStreamsHandler

  /**
   * Function that will be called to process a single stream.
   * The results of this function should be raw data fetched from external API
   * that will be processed by the processData function.
   *
   * Use ctx.processData to process data
   * Use ctx.publishStream to create a new stream if needed
   * @param ctx Everything that is needed to process a single stream
   */
  processStream: ProcessStreamHandler

  /**
   * Function that will be called to process a single webhook stream.
   * The results of this function should be raw data fetched from external API
   * that will be processed by the processData function.
   *
   * Use ctx.processData to process data
   * Use ctx.publishStream to create a new stream if needed
   * @param ctx Everything that is needed to process a single stream
   */
  processWebhookStream?: ProcessWebhookStreamHandler

  /**
   * Function that will be called to process a single stream data.
   * The results of this function should be activities (with member information).
   *
   * Use ctx.publishActivity to publish an activity
   * @param ctx Everything that is needed to process a single stream data
   */
  processData: ProcessDataHandler

  /**
   * Function that will be called in the end of successful integration run.
   * The result of this function should be new settings of the integration.
   * The new settings will be merged with the old settings and saved.
   *
   * @param settings current settings of the integration
   * @returns new settings of the integration
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  postProcess?: (settings: any) => any

  // type of integration service
  type: string

  // list of supported member attributes that this integration can generate
  memberAttributes: IMemberAttribute[]

  // how often to check for new data
  // if undefined it will never check
  // if 0 it will check the same as if it was 1 - every minute
  checkEvery?: number
}

export interface IIntegrationServiceSettings {
  nangoUrl: string
  nangoSecretKey: string
  nangoId: string
}
