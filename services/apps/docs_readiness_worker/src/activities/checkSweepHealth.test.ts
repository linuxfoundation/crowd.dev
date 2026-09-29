import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { checkIncrementalSweepHealth } from './checkSweepHealth'

const mocks = vi.hoisted(() => ({
  findLatestDocReadinessRun: vi.fn(),
  findStaleRunningDocReadinessRuns: vi.fn(),
  findLatestProjectDocReadinessUpdatedAt: vi.fn(),
  sendSlackNotificationAsync: vi.fn(),
}))

const READER = { name: 'reader' }
const WRITER = { name: 'writer' }

vi.mock('../main', () => ({
  svc: {
    postgres: {
      reader: { connection: () => READER },
      writer: { connection: () => WRITER },
    },
  },
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  pgpQx: vi.fn((connection) => connection),
}))

vi.mock('@crowd/data-access-layer', () => ({
  findLatestDocReadinessRun: mocks.findLatestDocReadinessRun,
  findStaleRunningDocReadinessRuns: mocks.findStaleRunningDocReadinessRuns,
  findLatestProjectDocReadinessUpdatedAt: mocks.findLatestProjectDocReadinessUpdatedAt,
}))

vi.mock('@crowd/slack', () => ({
  SlackChannel: { CDP_ALERTS: 'cdp-alerts' },
  SlackPersona: { WARNING_PROPAGATOR: 'warning' },
  sendSlackNotificationAsync: mocks.sendSlackNotificationAsync,
}))

const HOUR = 60 * 60 * 1000
const ago = (h: number) => new Date(Date.now() - h * HOUR)

function healthy() {
  mocks.findLatestDocReadinessRun.mockResolvedValue({
    id: 'run-ok',
    status: 'completed',
    startedAt: ago(20).toISOString(),
    finishedAt: ago(19).toISOString(),
    totalProjects: 250,
  })
  mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([])
  mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(ago(19))
}

beforeEach(healthy)
afterEach(() => vi.clearAllMocks())

describe('checkIncrementalSweepHealth', () => {
  test('posts nothing when everything is fresh', async () => {
    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).not.toHaveBeenCalled()
  })

  test('judges the sweep by its latest completed run, not by runs that failed or were closed', async () => {
    await checkIncrementalSweepHealth()

    expect(mocks.findLatestDocReadinessRun).toHaveBeenCalledWith(
      READER,
      'scheduled-incremental',
      'completed',
    )
  })

  test('alerts when the latest completed incremental run finished over 36h ago', async () => {
    mocks.findLatestDocReadinessRun.mockResolvedValue({
      id: 'run-old',
      status: 'completed',
      startedAt: ago(50).toISOString(),
      finishedAt: ago(49).toISOString(),
    })

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).toHaveBeenCalledTimes(1)
    expect(mocks.sendSlackNotificationAsync.mock.calls[0][3]).toContain(
      'Latest completed run `run-old`',
    )
  })

  test('alerts when no incremental run ever completed, e.g. after the only run was closed as failed', async () => {
    mocks.findLatestDocReadinessRun.mockResolvedValue(null)

    await checkIncrementalSweepHealth()

    const titles = mocks.sendSlackNotificationAsync.mock.calls.map((c) => c[2])
    expect(titles).toContain('Docs readiness incremental sweep hasn’t completed in over 36h')
    expect(mocks.sendSlackNotificationAsync.mock.calls[0][3]).toBe(
      'No completed scheduled-incremental run found at all.',
    )
  })

  test('alerts with run ids when runs are stuck in running for over 36h', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([
      { id: 'stuck-1', workflowId: 'wf-1', startedAt: ago(60) },
      { id: 'stuck-2', workflowId: null, startedAt: ago(40) },
    ])

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).toHaveBeenCalledTimes(1)
    const [channel, , title, detail] = mocks.sendSlackNotificationAsync.mock.calls[0]
    expect(channel).toBe('cdp-alerts')
    expect(title).toContain('2 run(s) stuck in running')
    expect(detail).toContain('stuck-1')
    expect(detail).toContain('stuck-2')
    expect(detail).toContain('workflow `n/a`')
    const cutoff = mocks.findStaleRunningDocReadinessRuns.mock.calls[0][1] as Date
    expect(Date.now() - cutoff.getTime()).toBeGreaterThanOrEqual(36 * HOUR)
  })

  test('alerts when no readiness row was written for over 36h', async () => {
    mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(ago(40))

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).toHaveBeenCalledTimes(1)
    expect(mocks.sendSlackNotificationAsync.mock.calls[0][2]).toContain(
      'not written a readiness row',
    )
  })

  test('does not alert on missing writes when the latest sweep completed with nothing to score', async () => {
    mocks.findLatestDocReadinessRun.mockResolvedValue({
      id: 'run-idle',
      status: 'completed',
      startedAt: ago(20).toISOString(),
      finishedAt: ago(19).toISOString(),
      totalProjects: 0,
    })
    mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(ago(200))

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).not.toHaveBeenCalled()
  })

  test('reads the stuck runs from the writer so rows the reaper just closed are not listed', async () => {
    await checkIncrementalSweepHealth()

    expect(mocks.findStaleRunningDocReadinessRuns.mock.calls[0][0]).toBe(WRITER)
  })

  test('the idle-sweep guard does not apply once that sweep is itself over 36h old', async () => {
    mocks.findLatestDocReadinessRun.mockResolvedValue({
      id: 'run-idle-old',
      status: 'completed',
      startedAt: ago(51).toISOString(),
      finishedAt: ago(50).toISOString(),
      totalProjects: 0,
    })
    mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(ago(200))

    await checkIncrementalSweepHealth()

    const titles = mocks.sendSlackNotificationAsync.mock.calls.map((c) => c[2])
    expect(titles).toEqual([
      'Docs readiness incremental sweep hasn’t completed in over 36h',
      'Docs readiness has not written a readiness row in over 36h',
    ])
  })

  test('the idle-sweep guard does not apply when the sweep did not record its project count', async () => {
    mocks.findLatestDocReadinessRun.mockResolvedValue({
      id: 'run-no-count',
      status: 'completed',
      startedAt: ago(20).toISOString(),
      finishedAt: ago(19).toISOString(),
      totalProjects: null,
    })
    mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(ago(200))

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).toHaveBeenCalledTimes(1)
    expect(mocks.sendSlackNotificationAsync.mock.calls[0][2]).toContain(
      'not written a readiness row',
    )
  })

  test('alerts when no readiness row exists at all', async () => {
    mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(null)

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync.mock.calls[0][3]).toContain('never')
  })

  test('posts one message per failing condition', async () => {
    mocks.findLatestDocReadinessRun.mockResolvedValue(null)
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([
      { id: 'stuck-1', workflowId: 'wf-1', startedAt: ago(60) },
    ])
    mocks.findLatestProjectDocReadinessUpdatedAt.mockResolvedValue(ago(100))

    await checkIncrementalSweepHealth()

    expect(mocks.sendSlackNotificationAsync).toHaveBeenCalledTimes(3)
  })
})
