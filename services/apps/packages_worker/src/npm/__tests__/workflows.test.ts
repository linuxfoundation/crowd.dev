import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  backfillDailyDownloads,
  backfillLast30dHistory,
  refreshLatestLast30dDownloads,
} from '../workflows'

const h = vi.hoisted(() => {
  const acts = {
    getLaneCount: vi.fn(),
    currentTimestamp: vi.fn(),
    backfillDailyLane: vi.fn(),
    refreshLatestLast30dLane: vi.fn(),
    backfillLast30dHistoryLane: vi.fn(),
    getUnscannedNpmBatch: vi.fn(),
    ingestNpmPackageBatch: vi.fn(),
    pollNpmChanges: vi.fn(),
    getChangedNpmPurls: vi.fn(),
    commitNpmChangesSeq: vi.fn(),
  }
  const proxyCalls: Array<{ options: Record<string, unknown>; activities: Set<string> }> = []
  return {
    acts,
    proxyCalls,
    proxyActivities: vi.fn((options: Record<string, unknown>) => {
      const call = { options, activities: new Set<string>() }
      proxyCalls.push(call)
      return new Proxy(acts, {
        get(target, name: string) {
          call.activities.add(name)
          return target[name as keyof typeof target]
        },
      })
    }),
    sleep: vi.fn(async () => undefined),
    continueAsNew: vi.fn(async () => undefined),
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
  }
})

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: h.proxyActivities,
  sleep: h.sleep,
  continueAsNew: h.continueAsNew,
  log: h.log,
}))

const CUTOFF = '2026-11-12T00:30:00.000Z'

beforeEach(() => {
  for (const fn of Object.values(h.acts)) fn.mockReset()
  h.sleep.mockClear()
  h.continueAsNew.mockClear()
  for (const fn of Object.values(h.log)) fn.mockClear()
  h.acts.getLaneCount.mockResolvedValue(1)
  h.acts.currentTimestamp.mockResolvedValue(CUTOFF)
})

function expectSleepThenContinue(state?: unknown) {
  expect(h.sleep).toHaveBeenCalledTimes(1)
  expect(h.sleep).toHaveBeenCalledWith('1 hour')
  expect(h.continueAsNew).toHaveBeenCalledTimes(1)
  if (state === undefined) {
    expect(h.continueAsNew.mock.calls[0]).toEqual([])
  } else {
    expect(h.continueAsNew).toHaveBeenCalledWith(state)
  }
  expect(h.sleep.mock.invocationCallOrder[0]).toBeLessThan(
    h.continueAsNew.mock.invocationCallOrder[0],
  )
}

describe('backfillDailyDownloads', () => {
  it('sleeps an hour on a blocked lane, then continues as new with the same cutoff', async () => {
    h.acts.backfillDailyLane.mockResolvedValue({ fetched: 20, blocked: true })

    await backfillDailyDownloads({ cutoff: CUTOFF })

    expect(h.acts.backfillDailyLane).toHaveBeenCalledTimes(1)
    expectSleepThenContinue({ cutoff: CUTOFF })
    expect(h.log.warn).toHaveBeenCalled()
  })

  it('runs another round without sleeping when a lane returns early with progress', async () => {
    h.acts.backfillDailyLane
      .mockResolvedValueOnce({ fetched: 20 })
      .mockResolvedValueOnce({ fetched: 0 })

    await backfillDailyDownloads({ cutoff: CUTOFF })

    expect(h.acts.backfillDailyLane).toHaveBeenCalledTimes(2)
    expect(h.sleep).not.toHaveBeenCalled()
    expect(h.continueAsNew).not.toHaveBeenCalled()
  })

  it('finishes without sleeping when a round fetches nothing', async () => {
    h.acts.backfillDailyLane.mockResolvedValue({ fetched: 0 })

    await backfillDailyDownloads({ cutoff: CUTOFF })

    expect(h.sleep).not.toHaveBeenCalled()
    expect(h.continueAsNew).not.toHaveBeenCalled()
  })
})

describe('refreshLatestLast30dDownloads', () => {
  it('sleeps an hour on a blocked lane, then continues as new with the same cutoff', async () => {
    h.acts.refreshLatestLast30dLane.mockResolvedValue({ fetched: 128, blocked: true })

    await refreshLatestLast30dDownloads({ cutoff: CUTOFF })

    expect(h.acts.refreshLatestLast30dLane).toHaveBeenCalledTimes(1)
    expectSleepThenContinue({ cutoff: CUTOFF })
    expect(h.log.warn).toHaveBeenCalled()
  })
})

describe('backfillLast30dHistory', () => {
  it('sleeps an hour on a blocked lane, then continues as new', async () => {
    h.acts.backfillLast30dHistoryLane.mockResolvedValue({ fetched: 50, blocked: true })

    await backfillLast30dHistory()

    expect(h.acts.backfillLast30dHistoryLane).toHaveBeenCalledTimes(1)
    expectSleepThenContinue()
    expect(h.log.warn).toHaveBeenCalled()
  })
})

describe('download lane activity options', () => {
  it.each(['backfillDailyLane', 'refreshLatestLast30dLane', 'backfillLast30dHistoryLane'])(
    '%s runs with a 1 hour startToClose and a 2 minute heartbeat timeout',
    async (activity) => {
      h.acts.backfillDailyLane.mockResolvedValue({ fetched: 0 })
      h.acts.refreshLatestLast30dLane.mockResolvedValue({ fetched: 0 })
      h.acts.backfillLast30dHistoryLane.mockResolvedValue({ fetched: 0 })

      await backfillDailyDownloads({ cutoff: CUTOFF })
      await refreshLatestLast30dDownloads({ cutoff: CUTOFF })
      await backfillLast30dHistory()

      const proxy = h.proxyCalls.find((call) => call.activities.has(activity))
      expect(proxy?.options).toMatchObject({
        startToCloseTimeout: '1 hour',
        heartbeatTimeout: '2 minutes',
      })
    },
  )
})

describe('elapsed time budget', () => {
  const HOUR = 60 * 60_000

  // Each lane call moves workflow time forward 5 hours, so the third round crosses 12 hours.
  function laneTakingFiveHours() {
    return async () => {
      vi.setSystemTime(Date.now() + 5 * HOUR)
      return { fetched: 20 }
    }
  }

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(new Date('2026-11-12T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('daily: continues as new with the same cutoff after 12 hours of rounds', async () => {
    h.acts.backfillDailyLane.mockImplementation(laneTakingFiveHours())

    await backfillDailyDownloads({ cutoff: CUTOFF })

    expect(h.acts.backfillDailyLane).toHaveBeenCalledTimes(3)
    expect(h.sleep).not.toHaveBeenCalled()
    expect(h.continueAsNew).toHaveBeenCalledTimes(1)
    expect(h.continueAsNew).toHaveBeenCalledWith({ cutoff: CUTOFF })
  })

  it('latest: continues as new with the same cutoff after 12 hours of rounds', async () => {
    h.acts.refreshLatestLast30dLane.mockImplementation(laneTakingFiveHours())

    await refreshLatestLast30dDownloads({ cutoff: CUTOFF })

    expect(h.acts.refreshLatestLast30dLane).toHaveBeenCalledTimes(3)
    expect(h.sleep).not.toHaveBeenCalled()
    expect(h.continueAsNew).toHaveBeenCalledWith({ cutoff: CUTOFF })
  })

  it('history: continues as new after 12 hours of rounds', async () => {
    h.acts.backfillLast30dHistoryLane.mockImplementation(laneTakingFiveHours())

    await backfillLast30dHistory()

    expect(h.acts.backfillLast30dHistoryLane).toHaveBeenCalledTimes(3)
    expect(h.sleep).not.toHaveBeenCalled()
    expect(h.continueAsNew).toHaveBeenCalledTimes(1)
    expect(h.continueAsNew.mock.calls[0]).toEqual([])
  })
})
