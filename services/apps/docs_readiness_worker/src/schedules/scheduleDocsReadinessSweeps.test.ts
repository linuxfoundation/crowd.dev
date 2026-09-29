import { ScheduleAlreadyRunning } from '@temporalio/client'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { MAX_CONCURRENT_WORKERS } from '../scoring/runChecksIsolated'
import { scheduleDocsReadinessSweeps } from './scheduleDocsReadinessSweeps'

const mocks = vi.hoisted(() => ({ create: vi.fn(), getHandle: vi.fn(), update: vi.fn() }))

vi.mock('../main', () => ({
  svc: {
    temporal: { schedule: { create: mocks.create, getHandle: mocks.getHandle } },
    log: { info: vi.fn() },
  },
}))

vi.mock('../workflows', () => ({
  runDocsReadinessSweep: function runDocsReadinessSweep() {},
  checkDocsReadinessSweepHealth: function checkDocsReadinessSweepHealth() {},
}))

const EXPECTED_TIMEOUTS = {
  docsReadinessFullSweep: '47 hours',
  docsReadinessIncrementalSweep: '23 hours',
  docsReadinessIncrementalSweepHealthCheck: '1 hour',
}

const EXPECTED_ARGS = {
  docsReadinessFullSweep: [{ mode: 'full', scope: 'lf', concurrency: MAX_CONCURRENT_WORKERS }],
  docsReadinessIncrementalSweep: [
    { mode: 'incremental', scope: 'lf', concurrency: MAX_CONCURRENT_WORKERS },
  ],
  docsReadinessIncrementalSweepHealthCheck: [],
}

afterEach(() => vi.resetAllMocks())

describe('scheduleDocsReadinessSweeps', () => {
  test('gives every new schedule an execution timeout so overlap SKIP cannot wedge it', async () => {
    await scheduleDocsReadinessSweeps()

    const timeouts = Object.fromEntries(
      mocks.create.mock.calls.map(([options]) => [
        options.scheduleId,
        options.action.workflowExecutionTimeout,
      ]),
    )
    expect(timeouts).toEqual(EXPECTED_TIMEOUTS)
  })

  test('starts no more sweep children than the scoring thread pool has threads', async () => {
    await scheduleDocsReadinessSweeps()

    const args = Object.fromEntries(
      mocks.create.mock.calls.map(([options]) => [options.scheduleId, options.action.args]),
    )
    expect(args).toEqual(EXPECTED_ARGS)
  })

  test('reconciles the action of schedules that already exist, keeping everything else', async () => {
    mocks.create.mockRejectedValue(new ScheduleAlreadyRunning('exists', 'id'))
    mocks.getHandle.mockReturnValue({ update: mocks.update })

    await scheduleDocsReadinessSweeps()

    expect(mocks.getHandle.mock.calls.map(([id]) => id)).toEqual(Object.keys(EXPECTED_TIMEOUTS))
    expect(mocks.update).toHaveBeenCalledTimes(3)

    const previous = {
      spec: { cronExpressions: ['0 3 * * *'] },
      policies: { catchupWindow: '1 minute' },
      state: { paused: true },
      action: { workflowExecutionTimeout: undefined },
    }
    const reconciled = mocks.update.mock.calls.map(([updater]) => updater(previous))
    expect(reconciled.map((r) => r.action.workflowExecutionTimeout)).toEqual(
      Object.values(EXPECTED_TIMEOUTS),
    )
    expect(reconciled.map((r) => r.action.args)).toEqual(Object.values(EXPECTED_ARGS))
    for (const result of reconciled) {
      expect(result).toMatchObject({
        spec: previous.spec,
        policies: previous.policies,
        state: previous.state,
      })
    }
  })

  test('rethrows any other error from creating a schedule', async () => {
    mocks.create.mockRejectedValue(new Error('server down'))

    await expect(scheduleDocsReadinessSweeps()).rejects.toThrow('server down')
    expect(mocks.update).not.toHaveBeenCalled()
  })
})
