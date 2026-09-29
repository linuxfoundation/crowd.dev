import { WorkflowNotFoundError } from '@temporalio/client'
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { closeStrandedRuns } from './closeStrandedRuns'

const READER = { name: 'reader' }
const WRITER = { name: 'writer' }

const mocks = vi.hoisted(() => ({
  findStaleRunningDocReadinessRuns: vi.fn(),
  finishDocReadinessRun: vi.fn(),
  sendSlackNotificationAsync: vi.fn(),
  describe: vi.fn(),
  getHandle: vi.fn(),
  warn: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: {
      reader: { connection: () => READER },
      writer: { connection: () => WRITER },
    },
    temporal: { workflow: { getHandle: mocks.getHandle } },
    log: { warn: mocks.warn },
  },
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({ pgpQx: vi.fn((conn) => conn) }))

vi.mock('@crowd/data-access-layer', () => ({
  findStaleRunningDocReadinessRuns: mocks.findStaleRunningDocReadinessRuns,
  finishDocReadinessRun: mocks.finishDocReadinessRun,
}))

vi.mock('@crowd/slack', () => ({
  SlackChannel: { CDP_ALERTS: 'cdp-alerts' },
  SlackPersona: { WARNING_PROPAGATOR: 'warning' },
  sendSlackNotificationAsync: mocks.sendSlackNotificationAsync,
}))

const run = (id: string, over = {}) => ({
  id,
  workflowId: `wf-${id}`,
  temporalRunId: `temporal-${id}`,
  startedAt: '2026-09-26T08:00:00.000Z',
  ...over,
})

const executionStatus = (name: string) => ({ status: { name } })
const notFound = () => new WorkflowNotFoundError('not found', 'wf', 'run')

beforeEach(() => {
  mocks.getHandle.mockReturnValue({ describe: mocks.describe })
  mocks.finishDocReadinessRun.mockImplementation(async (_qx, id) => ({ id }))
})

afterEach(() => vi.resetAllMocks())

describe('closeStrandedRuns', () => {
  test('does nothing and posts nothing when no run is running', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([])

    await closeStrandedRuns()

    expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
    expect(mocks.sendSlackNotificationAsync).not.toHaveBeenCalled()
  })

  test('reads the running rows from the reader and closes them through the writer', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
    mocks.describe.mockResolvedValue(executionStatus('TERMINATED'))
    const before = Date.now()

    await closeStrandedRuns()

    const [qx, cutoff] = mocks.findStaleRunningDocReadinessRuns.mock.calls[0]
    expect(qx).toBe(READER)
    expect((cutoff as Date).getTime()).toBeGreaterThanOrEqual(before)
    expect(mocks.finishDocReadinessRun.mock.calls[0][0]).toBe(WRITER)
  })

  test('never closes a run whose workflow execution is still running', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
    mocks.describe.mockResolvedValue(executionStatus('RUNNING'))

    await closeStrandedRuns()

    expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
    expect(mocks.sendSlackNotificationAsync).not.toHaveBeenCalled()
  })

  test.each(['TERMINATED', 'TIMED_OUT', 'FAILED', 'CANCELLED', 'COMPLETED'])(
    'closes a run whose workflow execution is %s',
    async (status) => {
      mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
      mocks.describe.mockResolvedValue(executionStatus(status))

      await closeStrandedRuns()

      expect(mocks.getHandle).toHaveBeenCalledWith('wf-a', 'temporal-a')
      expect(mocks.finishDocReadinessRun).toHaveBeenCalledWith(WRITER, 'a', {
        status: 'failed',
        errorMessage: `run never finished: workflow execution is ${status}`,
      })
    },
  )

  test.each(['PAUSED', 'UNKNOWN', 'UNSPECIFIED'])(
    'leaves a run alone when its status is %s',
    async (status) => {
      mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
      mocks.describe.mockResolvedValue(executionStatus(status))

      await closeStrandedRuns()

      expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
    },
  )

  test('closes a run whose workflow no longer exists in Temporal', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
    mocks.describe.mockRejectedValue(notFound())

    await closeStrandedRuns()

    expect(mocks.finishDocReadinessRun).toHaveBeenCalledWith(
      WRITER,
      'a',
      expect.objectContaining({ errorMessage: expect.stringContaining('NOT_FOUND') }),
    )
  })

  describe('continue-as-new chains (a sweep row keeps the first run id)', () => {
    const chain = (latest: string) =>
      mocks.describe
        .mockResolvedValueOnce(executionStatus('CONTINUED_AS_NEW'))
        .mockResolvedValueOnce(executionStatus(latest))

    test('alive while the latest run is running', async () => {
      mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('sweep')])
      chain('RUNNING')

      await closeStrandedRuns()

      expect(mocks.getHandle).toHaveBeenNthCalledWith(1, 'wf-sweep', 'temporal-sweep')
      expect(mocks.getHandle).toHaveBeenNthCalledWith(2, 'wf-sweep', undefined)
      expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
    })

    test.each(['PAUSED', 'UNKNOWN', 'UNSPECIFIED'])(
      'alive while the latest run is %s',
      async (latest) => {
        mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('sweep')])
        chain(latest)

        await closeStrandedRuns()

        expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
      },
    )

    test.each(['TIMED_OUT', 'FAILED', 'TERMINATED', 'COMPLETED', 'CANCELLED'])(
      'closed when the latest run is %s',
      async (latest) => {
        mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('sweep')])
        chain(latest)

        await closeStrandedRuns()

        expect(mocks.finishDocReadinessRun).toHaveBeenCalledWith(
          WRITER,
          'sweep',
          expect.objectContaining({ errorMessage: expect.stringContaining(latest) }),
        )
      },
    )

    test('closed as NOT_FOUND when the whole chain is gone', async () => {
      mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('sweep')])
      mocks.describe
        .mockResolvedValueOnce(executionStatus('CONTINUED_AS_NEW'))
        .mockRejectedValueOnce(notFound())

      await closeStrandedRuns()

      expect(mocks.finishDocReadinessRun).toHaveBeenCalledWith(
        WRITER,
        'sweep',
        expect.objectContaining({ errorMessage: expect.stringContaining('NOT_FOUND') }),
      )
    })
  })

  describe('a run whose own history is gone', () => {
    test('stays alive while the workflow id has a live latest run (a chain older than retention)', async () => {
      mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('sweep')])
      mocks.describe
        .mockRejectedValueOnce(notFound())
        .mockResolvedValueOnce(executionStatus('RUNNING'))

      await closeStrandedRuns()

      expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
    })

    test.each([['closed', 'FAILED'] as const, ['gone', null] as const])(
      'is closed as NOT_FOUND when the latest run is %s',
      async (_label, latest) => {
        mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
        mocks.describe.mockRejectedValueOnce(notFound())
        if (latest) {
          mocks.describe.mockResolvedValueOnce(executionStatus(latest))
        } else {
          mocks.describe.mockRejectedValueOnce(notFound())
        }

        await closeStrandedRuns()

        expect(mocks.finishDocReadinessRun).toHaveBeenCalledWith(
          WRITER,
          'a',
          expect.objectContaining({ errorMessage: expect.stringContaining('NOT_FOUND') }),
        )
      },
    )
  })

  test('looks up the latest execution when the row has no temporal run id', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a', { temporalRunId: null })])
    mocks.describe.mockResolvedValue(executionStatus('TERMINATED'))

    await closeStrandedRuns()

    expect(mocks.getHandle).toHaveBeenCalledWith('wf-a', undefined)
    expect(mocks.finishDocReadinessRun).toHaveBeenCalledTimes(1)
  })

  test('skips rows without a workflow id', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a', { workflowId: null })])

    await closeStrandedRuns()

    expect(mocks.getHandle).not.toHaveBeenCalled()
    expect(mocks.finishDocReadinessRun).not.toHaveBeenCalled()
  })

  test('keeps going when Temporal cannot be reached for one row', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a'), run('b')])
    mocks.describe
      .mockRejectedValueOnce(new Error('connection refused'))
      .mockResolvedValueOnce(executionStatus('TERMINATED'))

    await closeStrandedRuns()

    expect(mocks.warn).toHaveBeenCalledTimes(1)
    expect(mocks.warn.mock.calls[0][1]).toBe('could not check docs readiness run against Temporal')
    expect(mocks.finishDocReadinessRun).toHaveBeenCalledTimes(1)
    expect(mocks.finishDocReadinessRun.mock.calls[0][1]).toBe('b')
  })

  test('reports a database failure as such and keeps closing the other runs', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a'), run('b')])
    mocks.describe.mockResolvedValue(executionStatus('TERMINATED'))
    mocks.finishDocReadinessRun
      .mockRejectedValueOnce(new Error('writer down'))
      .mockImplementationOnce(async (_qx, id) => ({ id }))

    await closeStrandedRuns()

    expect(mocks.warn).toHaveBeenCalledTimes(1)
    expect(mocks.warn.mock.calls[0][1]).toBe('could not close stranded docs readiness run')
    expect(mocks.sendSlackNotificationAsync.mock.calls[0][2]).toBe(
      'Docs readiness closed 1 run(s) that never finished',
    )
  })

  test('does not count a run that another writer finished first', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a')])
    mocks.describe.mockResolvedValue(executionStatus('TERMINATED'))
    mocks.finishDocReadinessRun.mockResolvedValue(null)

    await closeStrandedRuns()

    expect(mocks.sendSlackNotificationAsync).not.toHaveBeenCalled()
  })

  test('posts one summary listing every closed run', async () => {
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue([run('a'), run('b'), run('c')])
    mocks.describe
      .mockResolvedValueOnce(executionStatus('TERMINATED'))
      .mockResolvedValueOnce(executionStatus('RUNNING'))
      .mockResolvedValueOnce(executionStatus('TIMED_OUT'))

    await closeStrandedRuns()

    expect(mocks.sendSlackNotificationAsync).toHaveBeenCalledTimes(1)
    const [channel, , title, detail] = mocks.sendSlackNotificationAsync.mock.calls[0]
    expect(channel).toBe('cdp-alerts')
    expect(title).toBe('Docs readiness closed 2 run(s) that never finished')
    expect(detail).toContain('`a` (workflow `wf-a`, TERMINATED)')
    expect(detail).toContain('`c` (workflow `wf-c`, TIMED_OUT)')
    expect(detail).not.toContain('`b`')
  })

  test('caps the listed runs in the summary', async () => {
    const runs = Array.from({ length: 25 }, (_, i) => run(`r${i}`))
    mocks.findStaleRunningDocReadinessRuns.mockResolvedValue(runs)
    mocks.describe.mockResolvedValue(executionStatus('TERMINATED'))

    await closeStrandedRuns()

    const [, , title, detail] = mocks.sendSlackNotificationAsync.mock.calls[0]
    expect(title).toContain('25 run(s)')
    expect(detail.split('\n')).toHaveLength(21)
    expect(detail).toContain('…and 5 more')
  })
})
