import { describe, expect, it } from 'vitest'

import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import { addLeaseLapsedUnits, applyRunCaps } from './runCap'

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

describe('applyRunCaps', () => {
  it('admits units up to the cap and defers the rest', () => {
    const result = applyRunCaps(
      [unit('a', 'sessionize'), unit('b', 'sessionize')],
      { sessionize: 1 },
      {},
    )

    expect(ids(result.admitted)).toEqual(['a'])
    expect(ids(result.deferred)).toEqual(['b'])
  })

  it('defers every unit while runs already fill the cap', () => {
    const result = applyRunCaps(
      [unit('a', 'sessionize'), unit('b', 'sessionize')],
      { sessionize: 1 },
      { sessionize: 1 },
    )

    expect(result.admitted).toEqual([])
    expect(ids(result.deferred)).toEqual(['a', 'b'])
  })

  it('defers a unit whose lease lapsed without counting it a second time', () => {
    const result = applyRunCaps(
      [unit('a', 'sessionize', true), unit('b', 'sessionize')],
      { sessionize: 2 },
      { sessionize: 1 },
    )

    expect(ids(result.admitted)).toEqual(['b'])
    expect(ids(result.deferred)).toEqual(['a'])
  })

  it('admits every unit of an uncapped platform, lapsed lease or not, keeping input order', () => {
    const result = applyRunCaps(
      [
        unit('a', 'github'),
        unit('b', 'sessionize'),
        unit('c', 'github', true),
        unit('d', 'sessionize'),
        unit('e', 'github'),
      ],
      { sessionize: 1 },
      {},
    )

    expect(ids(result.admitted)).toEqual(['a', 'b', 'c', 'e'])
    expect(ids(result.deferred)).toEqual(['d'])
  })
})

describe('addLeaseLapsedUnits', () => {
  it('counts lease-lapsed units of capped platforms as running', () => {
    expect(
      addLeaseLapsedUnits(
        { sessionize: 1 },
        [
          unit('a', 'sessionize', true),
          unit('b', 'sessionize'),
          unit('c', 'github', true),
          unit('d', 'other', true),
        ],
        { sessionize: 1, other: 2 },
      ),
    ).toEqual({ sessionize: 2, other: 1 })
  })
})
