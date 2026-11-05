import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Options, ServiceWorker } from './worker'

const calls = vi.hoisted(() => [] as string[])

const fakeWorker = vi.hoisted(() => {
  let state = 'RUNNING'
  let finishRun: () => void = () => undefined
  return {
    getState: () => state,
    run: vi.fn(
      () =>
        new Promise<void>((resolve) => {
          state = 'RUNNING'
          calls.push('worker.run')
          finishRun = () => {
            state = 'STOPPED'
            calls.push('worker.drained')
            resolve()
          }
        }),
    ),
    shutdown: vi.fn(() => {
      state = 'DRAINING'
      calls.push('worker.shutdown')
    }),
    drain: () => finishRun(),
  }
})

const runtimeInstall = vi.hoisted(() => vi.fn())

vi.mock('@temporalio/worker', () => ({
  Runtime: { install: runtimeInstall },
  NativeConnection: {
    connect: vi.fn(async () => ({
      close: async () => {
        if (fakeWorker.getState() !== 'STOPPED') {
          throw new Error('Cannot close connection while Workers hold a reference to it')
        }
        calls.push('worker-connection.close')
      },
    })),
  },
  Worker: { create: vi.fn(async () => fakeWorker) },
  bundleWorkflowCode: vi.fn(async () => ({})),
  makeTelemetryFilterString: () => '',
}))

vi.mock('@crowd/archetype-standard', () => ({
  Service: class {
    log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() }
    name = 'test-worker'
    async init() {
      return undefined
    }
    async stop() {
      calls.push('temporal-client.close')
    }
  },
}))

vi.mock('@crowd/database', () => ({
  getDbConnection: vi.fn(async () => ({})),
  DbStore: class {
    dbInstance = { end: () => calls.push('postgres.end') }
  },
}))

vi.mock('@crowd/temporal', () => ({ getDataConverter: vi.fn(async () => ({})) }))
vi.mock('@crowd/opensearch', () => ({ OpenSearchService: class {}, getOpensearchClient: vi.fn() }))
vi.mock('@crowd/queue', () => ({ QueueFactory: { createQueueService: vi.fn() } }))
vi.mock('@crowd/slack', () => ({ SlackChannel: {} }))
vi.mock('@crowd/logging', () => ({ getServiceChildLogger: () => ({}) }))
vi.mock('./logging', () => ({ createTemporalLogger: () => ({}) }))
vi.mock('./activities', () => ({}))
vi.mock('./activities/activityInterceptor', () => ({ ActivityMonitoringInterceptor: class {} }))

const ENV = {
  CROWD_TEMPORAL_SERVER_URL: 'temporal:7233',
  CROWD_TEMPORAL_NAMESPACE: 'default',
  CROWD_TEMPORAL_TASKQUEUE: 'test',
  CROWD_TEMPORAL_ENCRYPTION_KEY_ID: 'k',
  CROWD_TEMPORAL_ENCRYPTION_KEY: 'v',
  CROWD_DB_READ_HOST: 'db',
  CROWD_DB_WRITE_HOST: 'db',
  CROWD_DB_PORT: '5432',
  CROWD_DB_USERNAME: 'u',
  CROWD_DB_PASSWORD: 'p',
  CROWD_DB_DATABASE: 'd',
}

function createServiceDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'archetype-worker-'))
  fs.mkdirSync(path.join(dir, 'src', 'activities'), { recursive: true })
  fs.writeFileSync(path.join(dir, 'src', 'activities', 'index.js'), 'module.exports = {}')
  return dir
}

describe('ServiceWorker.stop', () => {
  let serviceDir: string

  beforeEach(() => {
    for (const [key, value] of Object.entries(ENV)) {
      vi.stubEnv(key, value)
    }
    serviceDir = createServiceDir()
    vi.spyOn(process, 'cwd').mockReturnValue(serviceDir)
    fakeWorker.run.mockClear()
    fakeWorker.shutdown.mockClear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllEnvs()
    fs.rmSync(serviceDir, { recursive: true, force: true })
    calls.length = 0
  })

  async function createService(options: Partial<Options> = {}) {
    const svc = new ServiceWorker(
      { producer: { enabled: false }, temporal: { enabled: true }, redis: { enabled: false } },
      { postgres: { enabled: true }, ...options },
    )
    await svc.init()
    return svc
  }

  it('drains the temporal worker before closing postgres and the temporal client', async () => {
    const svc = await createService()
    const started = svc.start()

    const stopped = svc['stop']()
    await Promise.resolve()

    expect(runtimeInstall).toHaveBeenCalledWith(expect.objectContaining({ shutdownSignals: [] }))
    expect(calls).toEqual(['worker.run', 'worker.shutdown'])

    fakeWorker.drain()
    await Promise.all([stopped, started])

    expect(calls).toEqual([
      'worker.run',
      'worker.shutdown',
      'worker.drained',
      'postgres.end',
      'postgres.end',
      'worker-connection.close',
      'temporal-client.close',
    ])
  })

  it('releases a worker that never started and skips start when stop arrives first', async () => {
    const svc = await createService()

    const stopped = svc['stop']()
    await Promise.resolve()
    fakeWorker.drain()
    await stopped
    await svc.start()

    expect(fakeWorker.run).toHaveBeenCalledTimes(1)
    expect(calls).toEqual([
      'worker.run',
      'worker.shutdown',
      'worker.drained',
      'postgres.end',
      'postgres.end',
      'worker-connection.close',
      'temporal-client.close',
    ])
  })

  it('closes postgres after the drain timeout so stuck activities fail fast, then waits for the run', async () => {
    vi.useFakeTimers()
    try {
      const svc = await createService({ drainTimeoutMs: 5_000 })
      const started = svc.start()

      const stopped = svc['stop']()
      await vi.advanceTimersByTimeAsync(5_000)

      expect(calls).toEqual(['worker.run', 'worker.shutdown', 'postgres.end', 'postgres.end'])

      fakeWorker.drain()
      await Promise.all([stopped, started])

      expect(calls).toEqual([
        'worker.run',
        'worker.shutdown',
        'postgres.end',
        'postgres.end',
        'worker.drained',
        'worker-connection.close',
        'temporal-client.close',
      ])
    } finally {
      vi.useRealTimers()
    }
  })
})
