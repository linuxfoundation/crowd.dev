import { Edition, QueuePriorityLevel } from '@crowd/types'

export function parseEnvInt(
  value: string | undefined,
  defaultValue: number,
  min: number,
  max: number,
): number {
  const parsed = parseInt(value ?? '', 10)
  return Number.isFinite(parsed) && parsed >= min && parsed <= max ? parsed : defaultValue
}

export const IS_TEST_ENV: boolean = process.env.NODE_ENV === 'test'

export const IS_DEV_ENV: boolean =
  process.env.NODE_ENV === 'development' ||
  process.env.NODE_ENV === 'docker' ||
  process.env.NODE_ENV === undefined

export const IS_PROD_ENV: boolean = process.env.NODE_ENV === 'production'

export const IS_STAGING_ENV: boolean = process.env.NODE_ENV === 'staging'

export const LOG_LEVEL: string = process.env.LOG_LEVEL || 'info'

export const IS_CLOUD_ENV: boolean = IS_PROD_ENV || IS_STAGING_ENV

export const IS_LLM_ENABLED: boolean = process.env.CROWD_ENABLE_LLM === 'true' || false

export const SERVICE = process.env.SERVICE || 'unknown-service'

export const EDITION: Edition = process.env.CROWD_EDITION as Edition

export const DATA_SINK_WORKER_PARTITIONS: Record<QueuePriorityLevel, number> = {
  [QueuePriorityLevel.HIGH]: process.env.DATA_SINK_WORKER_HIGH_PARTITIONS
    ? Number(process.env.DATA_SINK_WORKER_HIGH_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.NORMAL]: process.env.DATA_SINK_WORKER_NORMAL_PARTITIONS
    ? Number(process.env.DATA_SINK_WORKER_NORMAL_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.SYSTEM]: process.env.DATA_SINK_WORKER_SYSTEM_PARTITIONS
    ? Number(process.env.DATA_SINK_WORKER_SYSTEM_PARTITIONS)
    : undefined,
}

export const SEARCH_SYNC_WORKER_PARTITIONS: Record<QueuePriorityLevel, number> = {
  [QueuePriorityLevel.HIGH]: process.env.SEARCH_SYNC_WORKER_HIGH_PARTITIONS
    ? Number(process.env.SEARCH_SYNC_WORKER_HIGH_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.NORMAL]: process.env.SEARCH_SYNC_WORKER_NORMAL_PARTITIONS
    ? Number(process.env.SEARCH_SYNC_WORKER_NORMAL_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.SYSTEM]: process.env.SEARCH_SYNC_WORKER_SYSTEM_PARTITIONS
    ? Number(process.env.SEARCH_SYNC_WORKER_SYSTEM_PARTITIONS)
    : undefined,
}

export const INTEGRATION_RUN_WORKER_PARTITIONS: Record<QueuePriorityLevel, number> = {
  [QueuePriorityLevel.HIGH]: process.env.INTEGRATION_RUN_WORKER_HIGH_PARTITIONS
    ? Number(process.env.INTEGRATION_RUN_WORKER_HIGH_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.NORMAL]: process.env.INTEGRATION_RUN_WORKER_NORMAL_PARTITIONS
    ? Number(process.env.INTEGRATION_RUN_WORKER_NORMAL_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.SYSTEM]: process.env.INTEGRATION_RUN_WORKER_SYSTEM_PARTITIONS
    ? Number(process.env.INTEGRATION_RUN_WORKER_SYSTEM_PARTITIONS)
    : undefined,
}

export const INTEGRATION_STREAM_WORKER_PARTITIONS: Record<QueuePriorityLevel, number> = {
  [QueuePriorityLevel.HIGH]: process.env.INTEGRATION_STREAM_WORKER_HIGH_PARTITIONS
    ? Number(process.env.INTEGRATION_STREAM_WORKER_HIGH_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.NORMAL]: process.env.INTEGRATION_STREAM_WORKER_NORMAL_PARTITIONS
    ? Number(process.env.INTEGRATION_STREAM_WORKER_NORMAL_PARTITIONS)
    : undefined,
  [QueuePriorityLevel.SYSTEM]: process.env.INTEGRATION_STREAM_WORKER_SYSTEM_PARTITIONS
    ? Number(process.env.INTEGRATION_STREAM_WORKER_SYSTEM_PARTITIONS)
    : undefined,
}

export const DISCOVERY_NEW_PROJECTS_LIMIT = parseEnvInt(
  process.env.CROWD_DISCOVERY_NEW_PROJECTS_LIMIT,
  20,
  1,
  10_000,
)

export function getEnv() {
  if (IS_PROD_ENV) return 'prod'
  if (IS_STAGING_ENV) return 'staging'
  return 'local'
}

export const DEFAULT_TENANT_ID = '875c38bd-2b1b-4e91-ad07-0cfbabb4c49f'
