import { beforeEach, describe, expect, it, vi } from 'vitest'

import { admitByBudget, deferUnit, startRun } from './dispatcherActivities'

const SHORT_DEFER_AT = new Date('2026-10-16T12:01:00.000Z')

const mocks = vi.hoisted(() => ({
  start: vi.fn(),
  renewRunningUnitLease: vi.fn(),
  rescheduleUnit: vi.fn(),
  countRunningUnits: vi.fn(),
  findManifest: vi.fn(),
  hasHeadroom: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    temporal: {
      workflow: { start: mocks.start },
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
  const sessionizeUnit = (id: string) => ({
    ...UNIT,
    id,
    platform: 'sessionize',
    syncName: 'sessions',
    channelId: id,
    channelName: `event-${id}`,
  })

  it('admits uncapped units without counting running units', async () => {
    const units = [UNIT, { ...UNIT, id: 'unit-2', leaseLapsed: true }]

    expect(await admitByBudget(units)).toEqual({ admitted: units, deferred: [] })
    expect(mocks.countRunningUnits).not.toHaveBeenCalled()
  })

  it('defers units beyond the platform run cap', async () => {
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
    )
  })

  it('keeps units deferred for headroom alongside units deferred by the run cap', async () => {
    mocks.countRunningUnits.mockResolvedValue({ sessionize: 1 })
    mocks.hasHeadroom.mockImplementation(async () => mocks.hasHeadroom.mock.calls.length !== 1)
    const capped = sessionizeUnit('event-1')

    expect(await admitByBudget([UNIT, capped])).toEqual({
      admitted: [],
      deferred: [UNIT, capped],
    })
  })
})
