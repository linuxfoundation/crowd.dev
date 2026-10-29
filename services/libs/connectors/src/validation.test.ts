import { describe, expect, it } from 'vitest'

import type { AnyManifest, SourceCounter } from './types'
import { assertKnownCounterKeys, selectCounters } from './validation'

function counter(
  key: string,
  syncName: string,
  range: SourceCounter['cost']['range'],
  allTime: SourceCounter['cost']['all-time'],
): SourceCounter<unknown> {
  return {
    key,
    syncName,
    activityTypes: [],
    cost: { range, 'all-time': allTime },
    count: async () => 0,
  }
}

const a = counter('a', 's1', 'cheap', 'cheap')
const b = counter('b', 's1', 'expensive', null)
const c = counter('c', 's1', null, 'cheap')
const d = counter('d', 's2', 'cheap', 'cheap')

const manifest: AnyManifest = {
  platform: 'fake',
  syncs: [],
  discover: async () => [],
  counters: [a, b, c, d],
}

describe('selectCounters', () => {
  it('returns cheap counters of the sync for the window kind when no keys are requested', () => {
    expect(selectCounters(manifest, 's1', 'range', null)).toEqual([a])
    expect(selectCounters(manifest, 's1', 'all-time', null)).toEqual([a, c])
  })

  it('returns requested counters of the sync supported for the window kind', () => {
    expect(selectCounters(manifest, 's1', 'range', ['b', 'c', 'd'])).toEqual([b])
  })

  it('returns nothing for a manifest without counters', () => {
    const bare: AnyManifest = { platform: 'bare', syncs: [], discover: async () => [] }
    expect(selectCounters(bare, 's1', 'range', null)).toEqual([])
  })
})

describe('assertKnownCounterKeys', () => {
  it('throws on an unknown key', () => {
    expect(() => assertKnownCounterKeys(manifest, ['zz'])).toThrow(
      'unknown counter key zz for fake',
    )
  })

  it('accepts known keys', () => {
    expect(() => assertKnownCounterKeys(manifest, ['a', 'd'])).not.toThrow()
  })
})
