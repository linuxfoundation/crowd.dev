import EventEmitter from 'events'

import { createClient } from 'redis'

import { IS_DEV_ENV, IS_TEST_ENV, timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import { IRedisConfiguration, IRedisPubSubPair, RedisClient } from './types'

const log = getServiceChildLogger('redis')

let redisConfig: IRedisConfiguration | undefined = undefined

export const REDIS_CONFIG = (): IRedisConfiguration => {
  if (!redisConfig) {
    redisConfig = {
      username: process.env.CROWD_REDIS_USERNAME,
      password: process.env.CROWD_REDIS_PASSWORD,
      host: process.env.CROWD_REDIS_HOST,
      port: process.env.CROWD_REDIS_PORT,
    }
  }

  return redisConfig
}

let client: RedisClient | undefined
export const getRedisClient = async (
  config: IRedisConfiguration,
  exitOnError?: boolean,
): Promise<RedisClient> => {
  if (config.host) {
    if (client) return client
    const host = config.host
    const port = config.port

    log.info(`Creating new Redis client instance for Redis Server: ${host}:${port}!`)

    client = createClient({
      url: `redis://${config.username}:${config.password}@${host}:${port}`,
    }) as RedisClient

    if (exitOnError) {
      ;(client as unknown as EventEmitter).on('error', async (err) => {
        log.error(err, { host, port }, 'Redis client error!')

        if (
          [
            'ECONNRESET',
            'ECONNREFUSED',
            'UNCERTAIN_STATE',
            'NR_CLOSED',
            'CONNECTION_BROKEN',
            'NOAUTH',
          ].includes(err.code)
        ) {
          log.fatal(err, { host, port }, 'Fatal redis client connection error - exiting process!')
          await timeout(100)
          process.nextTick(() => process.exit())
        }
      })
    }

    await client.connect()
    await client.ping()
    log.info('Redis client connected!')

    return client
  }

  throw new Error('Redis client not configured!')
}

export const stopClient = async (client: RedisClient): Promise<string> => client.quit()

export const flushRedisContent = async (client: RedisClient): Promise<void> => {
  if (IS_DEV_ENV || IS_TEST_ENV) {
    log.warn('Flushing and entire redis!')
    await client.flushAll()
  }

  log.warn('Not development or test environment - nothing will be flushed!')
}

let pair: IRedisPubSubPair | undefined
export const getRedisPubSubPair = async (
  config: IRedisConfiguration,
): Promise<IRedisPubSubPair> => {
  if (config.host) {
    if (pair) return pair
    const host = config.host
    const port = config.port

    log.info(`Creating new Redis pub/sub client instances for Redis Server: ${host}:${port}!`)

    const pubClient = createClient({
      url: `redis://${config.username}:${config.password}@${host}:${port}`,
    }) as RedisClient
    const subClient = pubClient.duplicate() as RedisClient

    await pubClient.connect()
    await pubClient.ping()
    await subClient.connect()
    await subClient.ping()

    pair = {
      pubClient,
      subClient,
    }

    return pair
  }

  throw new Error('Redis client not configured!')
}

export const stopPubSubPair = async (pair: IRedisPubSubPair): Promise<void> => {
  await stopClient(pair.pubClient)
  await stopClient(pair.subClient)
}
