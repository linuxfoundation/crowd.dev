import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import type { IOnboardableSessionizeEvent } from '@crowd/data-access-layer/src/sessionize'
import type { Logger } from '@crowd/logging'

import { LIFECYCLE_STALE_LOCK_MS } from './githubLifecycle'
import {
  ONBOARDING_SPACING_SECONDS,
  onboardMatchedSessionizeEvents,
  onboardSessionizeEvent,
} from './onboardSessionizeEvent'

const mocks = vi.hoisted(() => ({
  hasActiveSyncUnitsForChannelElsewhere: vi.fn(),
  findSyncUnitByChannelId: vi.fn(),
  reviveDecommissionedSyncUnit: vi.fn(),
  lockSyncUnitChannel: vi.fn(),
  alignSyncUnitChannelName: vi.fn(),
  upsertSyncUnits: vi.fn(),
  findOrCreateSessionizeIntegration: vi.fn(),
  listOnboardableSessionizeEvents: vi.fn(),
  markSessionizeEventOnboarded: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  hasActiveSyncUnitsForChannelElsewhere: mocks.hasActiveSyncUnitsForChannelElsewhere,
  findSyncUnitByChannelId: mocks.findSyncUnitByChannelId,
  reviveDecommissionedSyncUnit: mocks.reviveDecommissionedSyncUnit,
  lockSyncUnitChannel: mocks.lockSyncUnitChannel,
  alignSyncUnitChannelName: mocks.alignSyncUnitChannelName,
  upsertSyncUnits: mocks.upsertSyncUnits,
}))

vi.mock('@crowd/data-access-layer/src/sessionize', () => ({
  findOrCreateSessionizeIntegration: mocks.findOrCreateSessionizeIntegration,
  listOnboardableSessionizeEvents: mocks.listOnboardableSessionizeEvents,
  markSessionizeEventOnboarded: mocks.markSessionizeEventOnboarded,
}))

const tx = vi.fn(async (fn: (inner: QueryExecutor) => Promise<unknown>) => fn(txQx))
const txQx = { name: 'tx', tx } as unknown as QueryExecutor
const qx = { tx } as unknown as QueryExecutor

function fakeLog() {
  const warn = vi.fn()
  return { warn, log: { warn } as unknown as Logger }
}

function matchedEvent(eventKey: string, segmentId: string): IOnboardableSessionizeEvent {
  return { eventKey, eventName: `Fake Summit ${eventKey}`, segmentId, integrationId: null }
}

describe('onboardSessionizeEvent', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(false)
    mocks.findSyncUnitByChannelId.mockResolvedValue(null)
  })

  it('creates one eventExport unit with no watermark and the given delay, under the channel lock', async () => {
    await expect(onboardSessionizeEvent(qx, 'int-1', '4101', 'Fake Summit', 240)).resolves.toEqual({
      outcome: 'created',
    })

    expect(mocks.lockSyncUnitChannel).toHaveBeenCalledWith(txQx, '4101')
    expect(mocks.hasActiveSyncUnitsForChannelElsewhere).toHaveBeenCalledWith(
      txQx,
      'int-1',
      '4101',
      expect.any(Date),
      'sessionize',
    )
    const staleLockBefore: Date = mocks.hasActiveSyncUnitsForChannelElsewhere.mock.calls[0][3]
    expect(Date.now() - staleLockBefore.getTime()).toBeGreaterThanOrEqual(LIFECYCLE_STALE_LOCK_MS)
    expect(mocks.findSyncUnitByChannelId).toHaveBeenCalledWith(
      txQx,
      'int-1',
      'sessionize',
      '4101',
      'eventExport',
    )
    expect(mocks.upsertSyncUnits).toHaveBeenCalledWith(txQx, [
      {
        integrationId: 'int-1',
        platform: 'sessionize',
        channelId: '4101',
        channelName: 'Fake Summit',
        syncName: 'eventExport',
        emitEnabled: true,
        initialDelaySeconds: 240,
      },
    ])
  })

  it('writes nothing when another sessionize integration owns the event', async () => {
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(true)

    await expect(onboardSessionizeEvent(qx, 'int-1', '4101', 'Fake Summit', 120)).resolves.toEqual({
      outcome: 'skipped',
      reason: 'owned_by_other_integration',
      message: 'units exist for sessionize event 4101 under another integration',
    })
    expect(mocks.findSyncUnitByChannelId).not.toHaveBeenCalled()
    expect(mocks.alignSyncUnitChannelName).not.toHaveBeenCalled()
    expect(mocks.reviveDecommissionedSyncUnit).not.toHaveBeenCalled()
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
  })

  it.for([
    ['active', { outcome: 'skipped', reason: 'already_present' }, false],
    ['paused', { outcome: 'skipped', reason: 'already_present' }, false],
    ['decommissioned', { outcome: 'reactivated' }, true],
  ] as const)(
    'aligns the event name of an existing %s unit and never creates a second one',
    async ([status, expected, revived]) => {
      mocks.findSyncUnitByChannelId.mockResolvedValue({ id: 'unit-1', status })

      expect(
        await onboardSessionizeEvent(qx, 'int-1', '4101', 'Renamed Summit', 120),
      ).toMatchObject(expected)
      expect(mocks.alignSyncUnitChannelName).toHaveBeenCalledWith(
        txQx,
        'int-1',
        '4101',
        'Renamed Summit',
      )
      expect(mocks.reviveDecommissionedSyncUnit).toHaveBeenCalledTimes(revived ? 1 : 0)
      expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
    },
  )
})

describe('onboardMatchedSessionizeEvents', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(false)
    mocks.findSyncUnitByChannelId.mockResolvedValue(null)
    mocks.findOrCreateSessionizeIntegration.mockImplementation(async (_qx, segmentId: string) => ({
      id: `int-${segmentId}`,
      created: true,
      restored: false,
    }))
  })

  it('shares one integration per segment, spaces new units and marks each event in its unit transaction', async () => {
    mocks.listOnboardableSessionizeEvents.mockResolvedValue([
      matchedEvent('4101', 'seg-a'),
      matchedEvent('4102', 'seg-a'),
      matchedEvent('4103', 'seg-b'),
      matchedEvent('4104', 'seg-b'),
    ])
    mocks.findSyncUnitByChannelId.mockImplementation(async (_tx, _int, _platform, eventKey) => {
      if (eventKey === '4102') return { id: 'unit-2', status: 'active' }
      if (eventKey === '4103') return { id: 'unit-3', status: 'decommissioned' }
      return null
    })
    mocks.findOrCreateSessionizeIntegration.mockImplementation(async (_qx, segmentId: string) => ({
      id: `int-${segmentId}`,
      created: segmentId === 'seg-a',
      restored: segmentId === 'seg-b',
    }))

    expect(await onboardMatchedSessionizeEvents(qx, fakeLog().log)).toEqual({
      integrationsCreated: 1,
      integrationsRestored: 1,
      unitsCreated: 2,
      unitsRevived: 1,
      skipped: { owned_by_other_integration: 0 },
    })

    expect(mocks.findOrCreateSessionizeIntegration.mock.calls).toEqual([
      [qx, 'seg-a'],
      [qx, 'seg-b'],
    ])
    expect(
      mocks.upsertSyncUnits.mock.calls.map(([, [unit]]) => [
        unit.channelId,
        unit.initialDelaySeconds,
      ]),
    ).toEqual([
      ['4101', ONBOARDING_SPACING_SECONDS],
      ['4104', 2 * ONBOARDING_SPACING_SECONDS],
    ])
    expect(mocks.markSessionizeEventOnboarded.mock.calls).toEqual([
      [txQx, '4101', 'int-seg-a'],
      [txQx, '4102', 'int-seg-a'],
      [txQx, '4103', 'int-seg-b'],
      [txQx, '4104', 'int-seg-b'],
    ])
    expect(tx).toHaveBeenCalledTimes(8)
  })

  it('fails the event transaction when its mark fails, so its unit is rolled back with it', async () => {
    mocks.listOnboardableSessionizeEvents.mockResolvedValue([matchedEvent('4101', 'seg-a')])
    const markFailure = new Error('mark failed')
    mocks.markSessionizeEventOnboarded.mockRejectedValue(markFailure)
    const outerTx = vi.fn(async (fn: (inner: QueryExecutor) => Promise<unknown>) => {
      const inner = { tx: vi.fn(async (f: (q: QueryExecutor) => unknown) => f(txQx)) }
      return fn(inner as unknown as QueryExecutor)
    })

    await expect(
      onboardMatchedSessionizeEvents({ tx: outerTx } as unknown as QueryExecutor, fakeLog().log),
    ).rejects.toBe(markFailure)

    const eventTx = outerTx.mock.results[0].value
    await expect(eventTx).rejects.toBe(markFailure)
    expect(mocks.upsertSyncUnits).toHaveBeenCalledTimes(1)
  })

  it('logs and leaves unmarked an event owned by another integration', async () => {
    mocks.listOnboardableSessionizeEvents.mockResolvedValue([matchedEvent('4101', 'seg-a')])
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(true)
    const { warn, log } = fakeLog()

    expect(await onboardMatchedSessionizeEvents(qx, log)).toMatchObject({
      unitsCreated: 0,
      skipped: { owned_by_other_integration: 1 },
    })
    expect(warn).toHaveBeenCalledWith(
      expect.objectContaining({
        event: 'sessionize_onboarding_skipped',
        eventKey: '4101',
        reason: 'owned_by_other_integration',
      }),
      expect.any(String),
    )
    expect(mocks.markSessionizeEventOnboarded).not.toHaveBeenCalled()
  })

  it('writes nothing when no event is matched', async () => {
    mocks.listOnboardableSessionizeEvents.mockResolvedValue([])

    expect(await onboardMatchedSessionizeEvents(qx, fakeLog().log)).toEqual({
      integrationsCreated: 0,
      integrationsRestored: 0,
      unitsCreated: 0,
      unitsRevived: 0,
      skipped: { owned_by_other_integration: 0 },
    })
    expect(mocks.findOrCreateSessionizeIntegration).not.toHaveBeenCalled()
    expect(tx).not.toHaveBeenCalled()
  })
})
