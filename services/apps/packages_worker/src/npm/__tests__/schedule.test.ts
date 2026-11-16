import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import * as workflows from '../../workflows'
import {
  scheduleDailyDownloadsBackfill,
  scheduleLast30dHistoryBackfill,
  scheduleLatestLast30dRefresh,
  scheduleNpmIngest,
} from '../schedule'

const h = vi.hoisted(() => {
  const update = vi.fn()
  return {
    create: vi.fn(),
    update,
    getHandle: vi.fn(() => ({ update })),
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  }
})

vi.mock('../../service', () => ({
  svc: {
    temporal: { schedule: { create: h.create, getHandle: h.getHandle } },
    log: h.log,
  },
}))

vi.mock('../../workflows', () => ({
  ingestNpmPackages: function ingestNpmPackages() {},
  backfillDailyDownloads: function backfillDailyDownloads() {},
  refreshLatestLast30dDownloads: function refreshLatestLast30dDownloads() {},
  backfillLast30dHistory: function backfillLast30dHistory() {},
}))

const cases = [
  {
    name: 'scheduleNpmIngest',
    run: scheduleNpmIngest,
    scheduleId: 'npm-registry-ingest',
    cron: '15 3 * * *',
    workflowType: workflows.ingestNpmPackages,
    catchupWindow: '1 hour',
  },
  {
    name: 'scheduleDailyDownloadsBackfill',
    run: scheduleDailyDownloadsBackfill,
    scheduleId: 'npm-daily-downloads-backfill',
    cron: '30 3 * * *',
    workflowType: workflows.backfillDailyDownloads,
    catchupWindow: '1 hour',
  },
  {
    name: 'scheduleLatestLast30dRefresh',
    run: scheduleLatestLast30dRefresh,
    scheduleId: 'npm-last-30d-latest-refresh',
    cron: '0 4 1 * *',
    workflowType: workflows.refreshLatestLast30dDownloads,
    catchupWindow: '1 day',
  },
  {
    name: 'scheduleLast30dHistoryBackfill',
    run: scheduleLast30dHistoryBackfill,
    scheduleId: 'npm-last-30d-history-backfill',
    cron: '30 4 * * *',
    workflowType: workflows.backfillLast30dHistory,
    catchupWindow: '1 hour',
  },
]

// Shape of a schedule as Temporal describes it after drift: an interval spec plus a calendar
// spec, a stale action, and a paused state that must survive the reconcile.
const drifted = {
  spec: {
    intervals: [{ every: 86_400_000, offset: 1_800_000 }],
    calendars: [{ hour: [{ start: 4, end: 4, step: 1 }] }],
  },
  policies: {
    overlap: ScheduleOverlapPolicy.ALLOW_ALL,
    catchupWindow: 60_000,
    pauseOnFailure: false,
  },
  action: { type: 'startWorkflow', workflowType: 'stale', taskQueue: 'stale-queue' },
  state: { paused: true, note: 'paused by an operator' },
}

beforeEach(() => {
  h.create.mockReset()
  h.update.mockReset()
  h.getHandle.mockClear()
})

describe.each(cases)('$name', ({ run, scheduleId, cron, workflowType, catchupWindow }) => {
  it('reconciles an existing schedule: replaces the spec, updates the action, keeps it paused', async () => {
    h.create.mockRejectedValue(new ScheduleAlreadyRunning('exists', scheduleId))

    await run()

    expect(h.getHandle).toHaveBeenCalledWith(scheduleId)
    expect(h.update).toHaveBeenCalledTimes(1)
    const updater = h.update.mock.calls[0][0] as (prev: typeof drifted) => Record<string, unknown>
    const next = updater(drifted)

    expect(next.spec).toEqual({ cronExpressions: [cron] })
    expect(next.state).toEqual(drifted.state)
    expect(next.policies).toMatchObject({
      overlap: ScheduleOverlapPolicy.SKIP,
      catchupWindow,
    })
    expect(next.action).toMatchObject({
      type: 'startWorkflow',
      workflowType,
      taskQueue: 'npm-worker',
      workflowRunTimeout: '24 hours',
    })
  })

  it('creates the schedule when it does not exist yet', async () => {
    h.create.mockResolvedValue(undefined)

    await run()

    expect(h.create).toHaveBeenCalledWith(
      expect.objectContaining({ scheduleId, spec: { cronExpressions: [cron] } }),
    )
    expect(h.update).not.toHaveBeenCalled()
  })

  it('rethrows other errors', async () => {
    h.create.mockRejectedValue(new Error('temporal unavailable'))

    await expect(run()).rejects.toThrow('temporal unavailable')
  })
})
