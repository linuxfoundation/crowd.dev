import { beforeEach, describe, expect, it, vi } from 'vitest'

import { deferUnit, startRun } from './dispatcherActivities'

const SHORT_DEFER_AT = new Date('2026-10-16T12:01:00.000Z')

const mocks = vi.hoisted(() => ({
  start: vi.fn(),
  renewRunningUnitLease: vi.fn(),
  rescheduleUnit: vi.fn(),
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
