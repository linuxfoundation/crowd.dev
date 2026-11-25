import { describe, expect, it } from 'vitest'

import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import { applyRunCaps } from './runCap'

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

function capAll(units: IClaimedUnit[], running: Record<string, number> = {}) {
  return applyRunCaps(units, units, { sessionize: 1 }, running)
}

describe('applyRunCaps', () => {
  it('admits fresh units up to the cap and defers the rest', () => {
    const result = capAll([unit('a', 'sessionize'), unit('b', 'sessionize')])

    expect(ids(result.admitted)).toEqual(['a'])
    expect(ids(result.deferred)).toEqual(['b'])
  })

  it('defers every unit while running units fill the cap', () => {
    const result = capAll([unit('a', 'sessionize', true), unit('b', 'sessionize')], {
      sessionize: 1,
    })

    expect(result.admitted).toEqual([])
    expect(ids(result.deferred)).toEqual(['a', 'b'])
  })

  it('gives a free slot to a lease-lapsed unit before a fresh unit claimed ahead of it', () => {
    const result = capAll([unit('a', 'sessionize'), unit('b', 'sessionize', true)])

    expect(ids(result.admitted)).toEqual(['b'])
    expect(ids(result.deferred)).toEqual(['a'])
  })

  it('admits lease-lapsed units in claim order up to the cap', () => {
    const result = capAll([unit('a', 'sessionize', true), unit('b', 'sessionize', true)])

    expect(ids(result.admitted)).toEqual(['a'])
    expect(ids(result.deferred)).toEqual(['b'])
  })

  it('lets a lease-lapsed unit deferred for headroom keep its slot', () => {
    const lapsed = unit('a', 'sessionize', true)
    const fresh = unit('b', 'sessionize')

    const result = applyRunCaps([fresh], [lapsed, fresh], { sessionize: 1 }, {})

    expect(result.admitted).toEqual([])
    expect(ids(result.deferred)).toEqual(['b'])
  })

  it('admits every unit of an uncapped platform, lapsed lease or not, keeping input order', () => {
    const result = capAll([
      unit('a', 'github'),
      unit('b', 'sessionize'),
      unit('c', 'github', true),
      unit('d', 'sessionize'),
      unit('e', 'github'),
    ])

    expect(ids(result.admitted)).toEqual(['a', 'b', 'c', 'e'])
    expect(ids(result.deferred)).toEqual(['d'])
  })
})
