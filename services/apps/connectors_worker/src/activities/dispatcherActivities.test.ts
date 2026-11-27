import { beforeEach, describe, expect, it, vi } from 'vitest'

import { WorkflowNotFoundError } from '@crowd/temporal'

import { admitByBudget, deferUnit, startRun, syncRunWorkflowId } from './dispatcherActivities'

const SHORT_DEFER_AT = new Date('2026-10-16T12:01:00.000Z')

const mocks = vi.hoisted(() => ({
  start: vi.fn(),
  renewRunningUnitLease: vi.fn(),
  rescheduleUnit: vi.fn(),
  countRunningUnits: vi.fn(),
  findManifest: vi.fn(),
  hasHeadroom: vi.fn(),
  getHandle: vi.fn(),
  describe: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    temporal: {
      workflow: { start: mocks.start, getHandle: mocks.getHandle },
    },
  },
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  claimDueUnits: vi.fn(),
  renewRunningUnitLease: mocks.renewRunningUnitLease,
  rescheduleUnit: mocks.rescheduleUnit,
  countRunningUnits: mocks.countRunningUnits,
}))

vi.mock('@crowd/connectors', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/connectors')>()),
  findManifest: mocks.findManifest,
  createTokenPool: () => ({ hasHeadroom: mocks.hasHeadroom }),
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  dbStoreQx: vi.fn(() => ({})),
}))

vi.mock('../scheduling', () => ({
  shortDeferRunAt: () => SHORT_DEFER_AT,
}))

const UNIT = {
  id: 'unit-1',
  integrationId: 'integration-1',
  platform: 'github',
  syncName: 'issues',
  channelId: 'repo',
  channelName: 'https://github.com/org/repo',
  leaseLapsed: false,
}

beforeEach(() => {
  vi.clearAllMocks()
  mocks.renewRunningUnitLease.mockResolvedValue(1)
  mocks.hasHeadroom.mockResolvedValue(true)
  mocks.getHandle.mockReturnValue({ describe: mocks.describe })
  mocks.findManifest.mockImplementation((platform: string) =>
    platform === 'sessionize' ? { platform, maxConcurrentRuns: 1 } : { platform },
  )
})

describe('startRun', () => {
  it('starts a workflow without touching the claim', async () => {
    mocks.start.mockResolvedValue(undefined)

    expect(await startRun(UNIT)).toBe('started')
    expect(mocks.renewRunningUnitLease).not.toHaveBeenCalled()
    expect(mocks.rescheduleUnit).not.toHaveBeenCalled()
  })

  it('keeps a still-running unit claimed instead of releasing it', async () => {
    const alreadyStarted = new Error('already started')
    alreadyStarted.name = 'WorkflowExecutionAlreadyStartedError'
    mocks.start.mockRejectedValue(alreadyStarted)

    expect(await startRun(UNIT)).toBe('alreadyRunning')
    expect(mocks.renewRunningUnitLease).toHaveBeenCalledWith({}, UNIT.id)
    expect(mocks.rescheduleUnit).not.toHaveBeenCalled()
  })
})

describe('deferUnit', () => {
  it('releases a freshly claimed unit', async () => {
    await deferUnit(UNIT.id, false)

    expect(mocks.rescheduleUnit).toHaveBeenCalledWith({}, UNIT.id, SHORT_DEFER_AT)
    expect(mocks.renewRunningUnitLease).not.toHaveBeenCalled()
  })

  it('keeps the claim on a unit whose previous lease lapsed, as its run may still be going', async () => {
    await deferUnit(UNIT.id, true)

    expect(mocks.renewRunningUnitLease).toHaveBeenCalledWith({}, UNIT.id)
    expect(mocks.rescheduleUnit).not.toHaveBeenCalled()
  })
})

describe('admitByBudget', () => {
  const sessionizeUnit = (id: string, leaseLapsed = false) => ({
    ...UNIT,
    id,
    platform: 'sessionize',
    syncName: 'sessions',
    channelId: id,
    channelName: `event-${id}`,
    leaseLapsed,
  })
  const executionStatus = (name: string) => ({ status: { name } })
  const runsByWorkflowId = (live: string[]) => (workflowId: string) => ({
    describe: async () => {
      if (live.some((id) => workflowId.endsWith(`/${id}`))) return executionStatus('RUNNING')
      throw new WorkflowNotFoundError('not found', workflowId, undefined)
    },
  })
  const firstUnitLacksHeadroom = () =>
    mocks.hasHeadroom.mockResolvedValueOnce(false).mockResolvedValueOnce(true)

  it('admits uncapped units without a count query or a Temporal lookup', async () => {
    const units = [UNIT, { ...UNIT, id: 'unit-2', leaseLapsed: true }]

    expect(await admitByBudget(units)).toEqual({ admitted: units, deferred: [] })
    expect(mocks.countRunningUnits).not.toHaveBeenCalled()
    expect(mocks.getHandle).not.toHaveBeenCalled()
  })

  it('defers fresh units beyond the platform run cap without a Temporal lookup', async () => {
    mocks.countRunningUnits.mockResolvedValue({})
    const first = sessionizeUnit('event-1')
    const second = sessionizeUnit('event-2')

    expect(await admitByBudget([first, UNIT, second])).toEqual({
      admitted: [first, UNIT],
      deferred: [second],
    })
    expect(mocks.countRunningUnits).toHaveBeenCalledWith(
      {},
      ['sessionize'],
      [first.id, UNIT.id, second.id],
      65,
    )
    expect(mocks.getHandle).not.toHaveBeenCalled()
  })

  it.each(['RUNNING', 'PAUSED'])(
    'counts a lease-lapsed unit whose workflow is %s as running and defers it without starting a run',
    async (statusName) => {
      mocks.countRunningUnits.mockResolvedValue({})
      mocks.describe.mockResolvedValue(executionStatus(statusName))
      const live = sessionizeUnit('event-1', true)
      const fresh = sessionizeUnit('event-2')

      expect(await admitByBudget([live, fresh])).toEqual({
        admitted: [],
        deferred: [live, fresh],
      })
      expect(mocks.getHandle).toHaveBeenCalledWith(syncRunWorkflowId(live))
      expect(mocks.getHandle).toHaveBeenCalledTimes(1)
      expect(mocks.start).not.toHaveBeenCalled()
    },
  )

  it('admits an orphan whose workflow finished into a free slot', async () => {
    mocks.countRunningUnits.mockResolvedValue({})
    mocks.describe.mockResolvedValue(executionStatus('FAILED'))
    const orphan = sessionizeUnit('event-1', true)

    expect(await admitByBudget([orphan])).toEqual({ admitted: [orphan], deferred: [] })
  })

  it('releases an orphan that finds no free slot', async () => {
    mocks.countRunningUnits.mockResolvedValue({ sessionize: 1 })
    mocks.getHandle.mockImplementation(runsByWorkflowId([]))

    expect(await admitByBudget([sessionizeUnit('event-1', true)])).toEqual({
      admitted: [],
      deferred: [sessionizeUnit('event-1', false)],
    })
  })

  it('keeps one run for cap 1 when a live run and an orphan lapse in the same tick', async () => {
    mocks.countRunningUnits.mockResolvedValue({})
    mocks.getHandle.mockImplementation(runsByWorkflowId(['live']))
    const orphan = sessionizeUnit('orphan', true)
    const live = sessionizeUnit('live', true)

    expect(await admitByBudget([orphan, live])).toEqual({
      admitted: [],
      deferred: [sessionizeUnit('orphan', false), live],
    })
  })

  it('starts the second of two orphans locked out of phase once the first is released', async () => {
    mocks.getHandle.mockImplementation(runsByWorkflowId([]))
    const first = sessionizeUnit('event-1', true)
    const second = sessionizeUnit('event-2', true)

    mocks.countRunningUnits.mockResolvedValueOnce({ sessionize: 1 })
    expect(await admitByBudget([first])).toEqual({
      admitted: [],
      deferred: [sessionizeUnit('event-1', false)],
    })

    mocks.countRunningUnits.mockResolvedValueOnce({})
    expect(await admitByBudget([second])).toEqual({ admitted: [second], deferred: [] })
  })

  it('releases an orphan the headroom check deferred and lets a fresh unit take the slot', async () => {
    mocks.countRunningUnits.mockResolvedValue({})
    mocks.getHandle.mockImplementation(runsByWorkflowId([]))
    const orphan = sessionizeUnit('event-1', true)
    const fresh = sessionizeUnit('event-2')
    firstUnitLacksHeadroom()

    expect(await admitByBudget([orphan, fresh])).toEqual({
      admitted: [fresh],
      deferred: [sessionizeUnit('event-1', false)],
    })
  })

  it('counts a live unit the headroom check deferred and keeps its lapsed lease', async () => {
    mocks.countRunningUnits.mockResolvedValue({})
    mocks.getHandle.mockImplementation(runsByWorkflowId(['event-1']))
    const live = sessionizeUnit('event-1', true)
    const fresh = sessionizeUnit('event-2')
    firstUnitLacksHeadroom()

    expect(await admitByBudget([live, fresh])).toEqual({
      admitted: [],
      deferred: [live, fresh],
    })
  })

  it('fails when Temporal cannot say whether a lapsed run is live', async () => {
    mocks.countRunningUnits.mockResolvedValue({})
    mocks.describe.mockRejectedValue(new Error('temporal unavailable'))

    await expect(admitByBudget([sessionizeUnit('event-1', true)])).rejects.toThrow(
      'temporal unavailable',
    )
  })

  it('keeps uncapped units deferred for headroom alongside units deferred by the run cap', async () => {
    mocks.countRunningUnits.mockResolvedValue({ sessionize: 1 })
    const lapsedRepo = { ...UNIT, leaseLapsed: true }
    const capped = sessionizeUnit('event-1')
    firstUnitLacksHeadroom()

    expect(await admitByBudget([lapsedRepo, capped])).toEqual({
      admitted: [],
      deferred: [lapsedRepo, capped],
    })
  })
})
