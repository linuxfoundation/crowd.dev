import { describe, expect, it } from 'vitest'

import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import { GITHUB_SYNC_MAPPINGS } from '@crowd/nango'

import {
  evaluateBackUnitGate,
  evaluateDiffCleanliness,
  evaluateForwardUnitGate,
  evaluateNangoConsumption,
  groupUnitsForSweep,
  handoverPoint,
  handoverSince,
  recentDiffedDays,
  resolveSwitchTargets,
} from './syncPipelineSwitch'

const unit = (overrides: Partial<ISyncUnit> = {}): ISyncUnit =>
  ({
    id: 'u-1',
    status: 'active',
    emitEnabled: false,
    watermark: { phase: 'incremental' },
    ...overrides,
  }) as ISyncUnit

const cleanDay = (day: string) => ({ day, missingInShadowCount: 0, unsupportedSyncCount: 0 })

describe('resolveSwitchTargets', () => {
  it('defaults to every mapped github sync, each with its nango counterpart', () => {
    expect(resolveSwitchTargets()).toEqual(
      GITHUB_SYNC_MAPPINGS.map((m) => ({ syncName: m.connectorSync, nangoSync: m.nangoSync })),
    )
    expect(resolveSwitchTargets(['pull-request-review-comments', 'stars'])).toEqual([
      { syncName: 'pull-request-review-comments', nangoSync: 'pull-request-reviews' },
      { syncName: 'stars', nangoSync: null },
    ])
  })
})

describe('recentDiffedDays', () => {
  it('starts two days back because the nightly diff targets D-2', () => {
    expect(recentDiffedDays(new Date('2026-10-19T10:00:00Z'))).toEqual([
      '2026-10-17',
      '2026-10-16',
      '2026-10-15',
    ])
  })
})

describe('forward gate', () => {
  it.each([
    [null, 'refused', 'no sync unit'],
    [unit({ status: 'dead_letter' }), 'refused', "status is 'dead_letter'"],
    [unit({ watermark: { phase: 'backfill' } }), 'refused', "phase is 'backfill'"],
    [unit({ emitEnabled: true }), 'unchanged', 'already emitting'],
  ])('refuses or skips %o', (candidate, outcome, reason) => {
    expect(evaluateForwardUnitGate(candidate)).toMatchObject({
      ok: false,
      outcome,
      reason: expect.stringContaining(reason),
    })
  })

  it('passes an incremental shadow unit that is active, or paused by an earlier switch run', () => {
    expect(evaluateForwardUnitGate(unit())).toEqual({ ok: true })
    expect(evaluateForwardUnitGate(unit({ status: 'paused' }))).toEqual({ ok: true })
  })
})

describe('diff cleanliness', () => {
  const days = ['2026-10-17', '2026-10-16', '2026-10-15']

  it('requires a zero-missing row for every day, ignoring field mismatch and missing in nango', () => {
    const rows = days.map((day) => ({
      ...cleanDay(day),
      fieldMismatchCount: 40,
      missingInNangoCount: 7,
    }))
    expect(evaluateDiffCleanliness(days, rows)).toEqual({ ok: true })
    expect(evaluateDiffCleanliness(days, rows.slice(0, 2))).toMatchObject({
      outcome: 'refused',
      reason: 'no diff summary for 2026-10-15',
    })
    expect(
      evaluateDiffCleanliness(days, [rows[0], { ...rows[1], missingInShadowCount: 2 }, rows[2]]),
    ).toMatchObject({
      outcome: 'refused',
      reason: 'diff for 2026-10-16 has missingInShadow=2 unsupportedSync=0',
    })
  })
})

describe('nango consumption', () => {
  const now = new Date('2026-10-19T10:00:00Z')

  it('refuses when cdp has not consumed the connection within a day', () => {
    expect(evaluateNangoConsumption('2026-10-19T09:00:00Z', now)).toEqual({ ok: true })
    expect(evaluateNangoConsumption('2026-10-18T09:00:00Z', now)).toMatchObject({
      outcome: 'refused',
      reason: 'nango records last consumed at 2026-10-18T09:00:00Z, more than 24h ago',
    })
    expect(evaluateNangoConsumption(null, now)).toMatchObject({ outcome: 'refused' })
  })
})

describe('back gate', () => {
  it('only needs an emitting unit, and names the unit when it is already off', () => {
    expect(evaluateBackUnitGate(null)).toMatchObject({ outcome: 'refused' })
    expect(evaluateBackUnitGate(unit())).toMatchObject({ outcome: 'unchanged', unitId: 'u-1' })
    expect(evaluateBackUnitGate(unit({ emitEnabled: true }))).toEqual({ ok: true })
    expect(evaluateBackUnitGate(unit({ emitEnabled: true, status: 'paused' }))).toEqual({
      ok: true,
    })
    for (const emitEnabled of [true, false]) {
      expect(evaluateBackUnitGate(unit({ emitEnabled, status: 'decommissioned' }))).toEqual({
        ok: false,
        outcome: 'refused',
        reason: 'unit is decommissioned',
      })
    }
    expect(evaluateBackUnitGate(unit({ emitEnabled: true, status: 'dead_letter' }))).toEqual({
      ok: true,
    })
    expect(evaluateForwardUnitGate(unit({ emitEnabled: true }))).toMatchObject({ unitId: 'u-1' })
  })
})

describe('groupUnitsForSweep', () => {
  it('turns shadow units into one repo per channel with only its unswitched syncs', () => {
    const shadowUnit = (channelName: string, syncName: string, integrationId = 'int-1') => ({
      id: `${channelName}/${syncName}`,
      integrationId,
      channelName,
      syncName,
    })

    expect(
      groupUnitsForSweep([
        shadowUnit('https://github.com/Org/Repo', 'issues'),
        shadowUnit('https://github.com/other/thing', 'forks'),
        shadowUnit('https://github.com/org/repo', 'pull-requests', 'int-2'),
      ]),
    ).toEqual([
      { owner: 'Org', repo: 'Repo', syncNames: ['issues', 'pull-requests'] },
      { owner: 'other', repo: 'thing', syncNames: ['forks'] },
    ])
  })
})

describe('handover point', () => {
  it('hands over from the earlier of the nango run finish and cdp consumption, an hour back', () => {
    expect(handoverPoint('2026-10-19T11:00:00Z', '2026-10-19T08:00:00Z')).toBe(
      '2026-10-19T08:00:00Z',
    )
    expect(handoverPoint('2026-10-19T09:00:00Z', '2026-10-19T09:30:00Z')).toBe(
      '2026-10-19T09:00:00Z',
    )
    expect(handoverPoint(null, '2026-10-19T09:30:00Z')).toBeNull()
    expect(handoverPoint('2026-10-19T09:00:00Z', null)).toBeNull()
    expect(handoverSince('2026-10-19T09:00:00Z')).toBe('2026-10-19T08:00:00.000Z')
  })
})
