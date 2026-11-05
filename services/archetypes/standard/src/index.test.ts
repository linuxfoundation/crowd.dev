import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Service } from './index'

const calls = vi.hoisted(() => [] as string[])

vi.mock('@crowd/common_services', () => ({ DataSinkWorkerEmitter: class {} }))
vi.mock('@crowd/integrations', () => ({ INTEGRATION_SERVICES: [] }))
vi.mock('@crowd/logging', () => ({ getServiceLogger: () => ({ info: vi.fn() }) }))
vi.mock('@crowd/queue', () => ({
  QUEUE_CONFIG: () => ({}),
  QueueFactory: {
    createQueueService: () => ({
      disconnectProducer: async () => calls.push('kafka-producer.disconnect'),
    }),
  },
}))
vi.mock('@crowd/redis', () => ({
  getRedisClient: vi.fn(async () => ({})),
  stopClient: async () => calls.push('redis.quit'),
  acquireLock: vi.fn(),
  releaseLock: vi.fn(),
}))
vi.mock('@crowd/temporal', () => ({
  getTemporalClient: vi.fn(async () => ({
    connection: { close: async () => calls.push('temporal-client.close') },
  })),
}))

const ENV = {
  SERVICE: 'test-service',
  CROWD_KAFKA_BROKERS: 'kafka:9092',
  CROWD_TEMPORAL_SERVER_URL: 'temporal:7233',
  CROWD_TEMPORAL_NAMESPACE: 'default',
  CROWD_REDIS_HOST: 'redis',
  CROWD_REDIS_PORT: '6379',
  CROWD_REDIS_USERNAME: 'u',
  CROWD_REDIS_PASSWORD: 'p',
}

describe('Service.stop', () => {
  beforeEach(() => {
    for (const [key, value] of Object.entries(ENV)) {
      vi.stubEnv(key, value)
    }
    vi.spyOn(process, 'on').mockImplementation(() => process)
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllEnvs()
    calls.length = 0
  })

  it('closes every connection init opened so the process can exit', async () => {
    const svc = new Service({
      producer: { enabled: true },
      temporal: { enabled: true },
      redis: { enabled: true },
    })
    await svc.init()

    await svc['stop']()

    expect(calls).toEqual(['temporal-client.close', 'redis.quit', 'kafka-producer.disconnect'])
  })

  it('skips connections that were never opened', async () => {
    const svc = new Service({
      producer: { enabled: false },
      temporal: { enabled: true },
      redis: { enabled: true },
    })

    await svc['stop']()

    expect(calls).toEqual([])
  })
})
