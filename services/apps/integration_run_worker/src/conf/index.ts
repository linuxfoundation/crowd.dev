import config from 'config'

import { IDatabaseConfig } from '@crowd/data-access-layer/src/database'
import { ISearchSyncApiConfig } from '@crowd/opensearch'
import { IQueueClientConfig } from '@crowd/queue'
import { IRedisConfiguration } from '@crowd/redis'
import { QueuePriorityLevel } from '@crowd/types'

export interface INangoConfig {
  url: string
  secretKey: string
}

export interface ILokiDbConfig {
  url: string
  token: string
}

let queueConfig: IQueueClientConfig
export const QUEUE_CONFIG = (): IQueueClientConfig => {
  if (queueConfig) return queueConfig

  queueConfig = config.get<IQueueClientConfig>('queue')
  return queueConfig
}

let dbConfig: IDatabaseConfig
export const DB_CONFIG = (): IDatabaseConfig => {
  if (dbConfig) return dbConfig

  dbConfig = config.get<IDatabaseConfig>('db')
  return dbConfig
}

let redisConfig: IRedisConfiguration
export const REDIS_CONFIG = (): IRedisConfiguration => {
  if (redisConfig) return redisConfig

  redisConfig = config.get<IRedisConfiguration>('redis')
  return redisConfig
}

export interface IWorkerConfig {
  maxRetries: number
  queuePriorityLevel: QueuePriorityLevel
}

let workerConfig: IWorkerConfig
export const WORKER_CONFIG = (): IWorkerConfig => {
  if (workerConfig) return workerConfig

  workerConfig = config.get<IWorkerConfig>('worker')
  return workerConfig
}

let nangoConfig: INangoConfig
export const NANGO_CONFIG = (): INangoConfig => {
  if (nangoConfig) return nangoConfig

  nangoConfig = config.get<INangoConfig>('nango')
  return nangoConfig
}

const platformMap: Map<string, unknown | null> = new Map()
export const PLATFORM_CONFIG = (platform: string): unknown | undefined => {
  if (platformMap.has(platform)) {
    const value = platformMap.get(platform)

    if (value === null) {
      return undefined
    }

    return value
  }

  if (config.has(platform)) {
    const value = config.get(platform)
    platformMap.set(platform, value)
    return value
  } else {
    platformMap.set(platform, null)
    return undefined
  }
}

export const SEARCH_SYNC_API_CONFIG = (): ISearchSyncApiConfig => {
  return config.get<ISearchSyncApiConfig>('searchSyncApi')
}

let lokiDbConfig: ILokiDbConfig
export const LOKI_DB_CONFIG = (): ILokiDbConfig => {
  if (lokiDbConfig) return lokiDbConfig

  lokiDbConfig = config.get<ILokiDbConfig>('loki')
  return lokiDbConfig
}
