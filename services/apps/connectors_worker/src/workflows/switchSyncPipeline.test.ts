import { beforeEach, describe, expect, it, vi } from 'vitest'

import { switchSyncPipeline } from './switchSyncPipeline'

const activities = vi.hoisted(() => ({
  resolveSwitchTarget: vi.fn(),
  evaluateForwardGate: vi.fn(),
  evaluateBackGate: vi.fn(),
  quiesceUnit: vi.fn(),
  isUnitLocked: vi.fn(),
  activateUnit: vi.fn(),
  pauseNangoRepoSync: vi.fn(),
  resumeNangoRepoSync: vi.fn(),
  setUnitEmitEnabled: vi.fn(),
  readNangoHandoverPoint: vi.fn(),
  takeOverUnit: vi.fn(),
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => activities,
  sleep: vi.fn(() => Promise.resolve()),
  log: { info: vi.fn() },
}))

const target = {
  integrationId: 'int-1',
  connectionId: 'conn-1',
  channelName: 'https://github.com/o/r',
}
const sync = (name: string) => ({ syncName: name, nangoSync: name })
const ok = (syncName: string) => ({ ok: true, unitId: `u-${syncName}` })
const verdict = (outcome: 'refused' | 'unchanged', reason: string, syncName?: string) => ({
  ok: false,
  outcome,
  reason,
  ...(syncName ? { unitId: `u-${syncName}` } : {}),
})

const firstCall = (fn: { mock: { invocationCallOrder: number[] } }) =>
  fn.mock.invocationCallOrder[0]
const lastCall = (fn: { mock: { invocationCallOrder: number[] } }) =>
  fn.mock.invocationCallOrder[fn.mock.invocationCallOrder.length - 1]

describe('switchSyncPipeline', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    activities.resolveSwitchTarget.mockResolvedValue({
      target,
      syncs: [sync('issues'), sync('forks')],
    })
    activities.isUnitLocked.mockResolvedValue(false)
    activities.pauseNangoRepoSync.mockResolvedValue(undefined)
    activities.resumeNangoRepoSync.mockResolvedValue(undefined)
    activities.setUnitEmitEnabled.mockResolvedValue(undefined)
    activities.readNangoHandoverPoint.mockResolvedValue({ handoverAt: '2026-10-19T09:00:00Z' })
    activities.takeOverUnit.mockResolvedValue({ since: '2026-10-19T08:00:00.000Z' })
  })

  it('forward: quiesces, drains on the lock, re-gates, reads the handover point, pauses nango, takes the unit over, reactivates; a failed pause reactivates and never flips', async () => {
    activities.evaluateForwardGate.mockImplementation(async ({ syncName }) => ok(syncName))
    activities.isUnitLocked.mockResolvedValueOnce(true).mockResolvedValue(false)
    activities.pauseNangoRepoSync
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(
        Object.assign(new Error('Activity task failed'), { cause: new Error('nango 503') }),
      )

    const results = await switchSyncPipeline({ owner: 'o', repo: 'r', direction: 'connectors' })

    expect(results).toEqual([
      {
        syncName: 'issues',
        outcome: 'switched',
        reason: 'since rewound to 2026-10-19T08:00:00.000Z',
      },
      { syncName: 'forks', outcome: 'refused', reason: 'nango pause failed: nango 503' },
    ])
    const order = [
      activities.quiesceUnit,
      activities.isUnitLocked,
      activities.readNangoHandoverPoint,
      activities.pauseNangoRepoSync,
      activities.takeOverUnit,
      activities.activateUnit,
    ].map(firstCall)
    expect(order).toEqual([...order].sort((a, b) => a - b))
    expect(activities.evaluateForwardGate).toHaveBeenCalledTimes(4)
    expect(activities.evaluateForwardGate).toHaveBeenCalledWith({ ...target, syncName: 'issues' })
    expect(activities.isUnitLocked).toHaveBeenCalledTimes(3)
    expect(activities.setUnitEmitEnabled).not.toHaveBeenCalled()
    expect(activities.takeOverUnit).toHaveBeenCalledTimes(1)
    expect(activities.readNangoHandoverPoint).toHaveBeenCalledWith({
      ...target,
      syncName: 'issues',
      nangoSync: 'issues',
    })
    expect(activities.takeOverUnit).toHaveBeenCalledWith({
      unitId: 'u-issues',
      handoverAt: '2026-10-19T09:00:00Z',
    })
    expect(activities.activateUnit.mock.calls).toEqual([
      [{ unitId: 'u-issues' }],
      [{ unitId: 'u-forks' }],
    ])
  })

  it('forward: refusals, a stuck lock, a unit that changed while draining, a nango sync that never ran, a status read that keeps failing, a takeover that fails after the pause, and an emitting unit that gets healed', async () => {
    activities.resolveSwitchTarget.mockResolvedValue({
      target,
      syncs: [
        sync('issues'),
        { syncName: 'stars', nangoSync: null },
        sync('forks'),
        sync('pull-requests'),
        sync('issue-comments'),
        sync('pull-request-comments'),
        sync('pull-request-commits'),
        sync('discussions'),
      ],
    })
    activities.readNangoHandoverPoint.mockImplementation(async ({ nangoSync }) => {
      if (nangoSync === 'pull-request-comments') throw new Error('nango 502')
      return { handoverAt: nangoSync === 'issue-comments' ? null : '2026-10-19T09:00:00Z' }
    })
    activities.takeOverUnit.mockImplementation(async ({ unitId }) => {
      if (unitId === 'u-pull-request-commits') throw new Error('deadlock')
      return { since: null }
    })
    const seen: Record<string, number> = {}
    activities.evaluateForwardGate.mockImplementation(async ({ syncName }) => {
      seen[syncName] = (seen[syncName] ?? 0) + 1
      if (syncName === 'issues') return verdict('refused', 'no diff summary for 2026-10-15')
      if (syncName === 'discussions') return verdict('unchanged', 'already emitting', 'discussions')
      if (syncName === 'pull-requests' && seen[syncName] > 1) {
        return verdict('refused', "unit status is 'dead_letter' (expected active or paused)")
      }
      return ok(syncName)
    })
    activities.isUnitLocked.mockImplementation(async ({ unitId }) => unitId === 'u-forks')

    const results = await switchSyncPipeline({ owner: 'o', repo: 'r', direction: 'connectors' })

    expect(results).toEqual([
      { syncName: 'issues', outcome: 'refused', reason: 'no diff summary for 2026-10-15' },
      { syncName: 'stars', outcome: 'refused', reason: 'unknown github sync' },
      {
        syncName: 'forks',
        outcome: 'refused',
        reason: 'unit still locked after the drain timeout; rerun',
      },
      {
        syncName: 'pull-requests',
        outcome: 'refused',
        reason:
          "unit changed while draining: unit status is 'dead_letter' (expected active or paused)",
      },
      {
        syncName: 'issue-comments',
        outcome: 'refused',
        reason: 'nango sync has no completed run to hand over from',
      },
      {
        syncName: 'pull-request-comments',
        outcome: 'refused',
        reason: 'nango status read failed: nango 502',
      },
      {
        syncName: 'pull-request-commits',
        outcome: 'refused',
        reason: 'unit takeover failed: deadlock; nango resumed; unit back in shadow',
      },
      {
        syncName: 'discussions',
        outcome: 'unchanged',
        reason: 'already emitting; shadow records cleared',
      },
    ])
    expect(activities.pauseNangoRepoSync.mock.calls.map(([i]) => i.nangoSync)).toEqual([
      'pull-request-commits',
      'discussions',
    ])
    expect(activities.resumeNangoRepoSync).toHaveBeenCalledWith({
      connectionId: 'conn-1',
      nangoSync: 'pull-request-commits',
    })
    expect(activities.setUnitEmitEnabled).not.toHaveBeenCalled()
    expect(activities.takeOverUnit).toHaveBeenCalledTimes(2)
    expect(activities.takeOverUnit).toHaveBeenLastCalledWith({
      unitId: 'u-discussions',
      handoverAt: null,
    })
    expect(activities.activateUnit.mock.calls).toEqual([
      [{ unitId: 'u-forks' }],
      [{ unitId: 'u-pull-requests' }],
      [{ unitId: 'u-issue-comments' }],
      [{ unitId: 'u-pull-request-comments' }],
      [{ unitId: 'u-pull-request-commits' }],
      [{ unitId: 'u-discussions' }],
    ])
  })

  it('back: flips and reactivates before resuming, and reactivates and resumes a unit that is already off', async () => {
    activities.evaluateBackGate
      .mockResolvedValueOnce(ok('issues'))
      .mockResolvedValueOnce(verdict('unchanged', 'already syncing through nango', 'forks'))

    const results = await switchSyncPipeline({ owner: 'o', repo: 'r', direction: 'nango' })

    expect(results).toEqual([
      { syncName: 'issues', outcome: 'switched' },
      {
        syncName: 'forks',
        outcome: 'unchanged',
        reason: 'already syncing through nango (resume re-sent)',
      },
    ])
    expect(firstCall(activities.setUnitEmitEnabled)).toBeLessThan(
      firstCall(activities.activateUnit),
    )
    expect(lastCall(activities.activateUnit)).toBeLessThan(lastCall(activities.resumeNangoRepoSync))
    expect(activities.setUnitEmitEnabled).toHaveBeenCalledWith({
      unitId: 'u-issues',
      emitEnabled: false,
    })
    expect(activities.activateUnit.mock.calls).toEqual([
      [{ unitId: 'u-issues' }],
      [{ unitId: 'u-forks' }],
    ])
    expect(activities.resumeNangoRepoSync).toHaveBeenCalledTimes(2)
    expect(activities.quiesceUnit).not.toHaveBeenCalled()
  })

  it('back: never resumes nango for a unit the gate refuses', async () => {
    activities.evaluateBackGate.mockResolvedValue(
      verdict('refused', "unit status is 'decommissioned' (expected active or paused)"),
    )

    const results = await switchSyncPipeline({ owner: 'o', repo: 'r', direction: 'nango' })

    expect(results.length).toBeGreaterThan(0)
    expect(results.every(({ outcome }) => outcome === 'refused')).toBe(true)
    expect(activities.setUnitEmitEnabled).not.toHaveBeenCalled()
    expect(activities.activateUnit).not.toHaveBeenCalled()
    expect(activities.resumeNangoRepoSync).not.toHaveBeenCalled()
  })

  it('refuses every sync when the repo has no active github-nango mapping', async () => {
    activities.resolveSwitchTarget.mockResolvedValue({ target: null, syncs: [sync('issues')] })

    await expect(
      switchSyncPipeline({ owner: 'o', repo: 'r', direction: 'connectors' }),
    ).resolves.toEqual([
      { syncName: 'issues', outcome: 'refused', reason: 'no active github-nango mapping for o/r' },
    ])
    expect(activities.evaluateForwardGate).not.toHaveBeenCalled()
  })
})
