import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { checkDocsReadinessSweepHealth } from './checkDocsReadinessSweepHealth'

const mocks = vi.hoisted(() => ({
  closeStrandedRuns: vi.fn(),
  checkIncrementalSweepHealth: vi.fn(),
  warn: vi.fn(),
  patched: vi.fn(),
}))

vi.mock('@temporalio/workflow', () => ({
  log: { warn: mocks.warn },
  patched: mocks.patched,
  rootCause: (err: Error & { cause?: Error }) => err.cause?.message,
  proxyActivities: () => ({
    closeStrandedRuns: mocks.closeStrandedRuns,
    checkIncrementalSweepHealth: mocks.checkIncrementalSweepHealth,
  }),
}))

vi.mock('../activities', () => ({}))

beforeEach(() => mocks.patched.mockReturnValue(true))

afterEach(() => vi.resetAllMocks())

describe('checkDocsReadinessSweepHealth', () => {
  test('closes stranded runs before running the health check', async () => {
    const order: string[] = []
    mocks.closeStrandedRuns.mockImplementation(async () => void order.push('close'))
    mocks.checkIncrementalSweepHealth.mockImplementation(async () => void order.push('check'))

    await checkDocsReadinessSweepHealth()

    expect(order).toEqual(['close', 'check'])
  })

  test('still runs the health check when closing stranded runs fails', async () => {
    mocks.closeStrandedRuns.mockRejectedValue(
      Object.assign(new Error('Activity task failed'), {
        cause: new Error('temporal unreachable'),
      }),
    )

    await checkDocsReadinessSweepHealth()

    expect(mocks.warn).toHaveBeenCalledWith('closing stranded docs readiness runs failed', {
      error: 'temporal unreachable',
    })
    expect(mocks.checkIncrementalSweepHealth).toHaveBeenCalledTimes(1)
  })

  test('skips the new activity for an execution that was already running before the deploy', async () => {
    mocks.patched.mockReturnValue(false)

    await checkDocsReadinessSweepHealth()

    expect(mocks.patched).toHaveBeenCalledWith('close-stranded-runs')
    expect(mocks.closeStrandedRuns).not.toHaveBeenCalled()
    expect(mocks.checkIncrementalSweepHealth).toHaveBeenCalledTimes(1)
  })

  test('propagates a failure of the health check itself', async () => {
    mocks.checkIncrementalSweepHealth.mockRejectedValue(new Error('db down'))

    await expect(checkDocsReadinessSweepHealth()).rejects.toThrow('db down')
  })
})
