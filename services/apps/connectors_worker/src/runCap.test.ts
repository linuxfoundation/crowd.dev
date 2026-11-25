import { describe, expect, it } from 'vitest'

import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import { applyRunCaps, releaseOrphan } from './runCap'

function unit(id: string, platform: string, leaseLapsed = false): IClaimedUnit {
  return {
    id,
    integrationId: 'integration-1',
    platform,
    syncName: 'sessions',
    channelId: id,
    channelName: `event-${id}`,
    leaseLapsed,
  }
}

const ids = (units: IClaimedUnit[]) => units.map((u) => u.id)
const CAPS = { sessionize: 1 }

function capAll(
  units: IClaimedUnit[],
  running: Record<string, number> = {},
  liveUnitIds: string[] = [],
) {
  return applyRunCaps(units, units, CAPS, running, new Set(liveUnitIds))
}

describe('applyRunCaps', () => {
  it('admits fresh units up to the cap and defers the rest', () => {
    const result = capAll([unit('a', 'sessionize'), unit('b', 'sessionize')])

    expect(ids(result.admitted)).toEqual(['a'])
    expect(ids(result.deferred)).toEqual(['b'])
  })

  it('defers fresh units while running units fill the cap', () => {
    const result = capAll([unit('a', 'sessionize')], { sessionize: 1 })

    expect(result.admitted).toEqual([])
    expect(ids(result.deferred)).toEqual(['a'])
  })

  it('counts a live lease-lapsed unit as running and defers it with its lapsed lease', () => {
    const live = unit('a', 'sessionize', true)
    const fresh = unit('b', 'sessionize')

    const result = capAll([fresh, live], {}, ['a'])

    expect(result.admitted).toEqual([])
    expect(result.deferred).toEqual([fresh, live])
  })

  it('counts a live lease-lapsed unit the headroom check deferred', () => {
    const live = unit('a', 'sessionize', true)
    const fresh = unit('b', 'sessionize')

    const result = applyRunCaps([fresh], [live, fresh], CAPS, {}, new Set(['a']))

    expect(result).toEqual({ admitted: [], deferred: [fresh] })
  })

  it('admits an orphan into a free slot', () => {
    const orphan = unit('a', 'sessionize', true)

    expect(capAll([orphan])).toEqual({ admitted: [orphan], deferred: [] })
  })

  it('releases an orphan that finds no free slot', () => {
    const result = capAll([unit('a', 'sessionize', true)], { sessionize: 1 })

    expect(result).toEqual({ admitted: [], deferred: [unit('a', 'sessionize', false)] })
  })

  it('lets orphans and fresh units compete for slots in claim order', () => {
    const result = applyRunCaps(
      [unit('a', 'sessionize'), unit('b', 'sessionize', true)],
      [unit('a', 'sessionize'), unit('b', 'sessionize', true)],
      { sessionize: 1 },
      {},
      new Set(),
    )

    expect(ids(result.admitted)).toEqual(['a'])
    expect(result.deferred).toEqual([unit('b', 'sessionize', false)])
  })

  it('keeps one run for cap 1 when a live run and an orphan lapse in the same tick', () => {
    const orphan = unit('orphan', 'sessionize', true)
    const live = unit('live', 'sessionize', true)

    const result = capAll([orphan, live], {}, ['live'])

    expect(result.admitted).toEqual([])
    expect(result.deferred).toEqual([unit('orphan', 'sessionize', false), live])
  })

  it('admits every unit of an uncapped platform untouched, keeping input order', () => {
    const result = capAll([
      unit('a', 'github'),
      unit('b', 'sessionize'),
      unit('c', 'github', true),
      unit('d', 'sessionize'),
      unit('e', 'github'),
    ])

    expect(result.admitted).toEqual([
      unit('a', 'github'),
      unit('b', 'sessionize'),
      unit('c', 'github', true),
      unit('e', 'github'),
    ])
    expect(ids(result.deferred)).toEqual(['d'])
  })
})

describe('releaseOrphan', () => {
  it('clears the lapsed lease of an orphan on a capped platform only', () => {
    const live = new Set(['live'])

    expect(releaseOrphan(unit('orphan', 'sessionize', true), CAPS, live).leaseLapsed).toBe(false)
    expect(releaseOrphan(unit('live', 'sessionize', true), CAPS, live).leaseLapsed).toBe(true)
    expect(releaseOrphan(unit('repo', 'github', true), CAPS, live).leaseLapsed).toBe(true)
  })
})
