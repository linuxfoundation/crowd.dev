import type { CountWindow } from '@crowd/data-access-layer/src/connectors/types'

import type { AnyManifest, SourceCounter } from './types'

export function selectCounters(
  manifest: AnyManifest,
  syncName: string,
  windowKind: CountWindow['kind'],
  requestedKeys: string[] | null,
): SourceCounter<unknown>[] {
  return (manifest.counters ?? []).filter((counter) => {
    if (counter.syncName !== syncName) {
      return false
    }
    return requestedKeys === null
      ? counter.cost[windowKind] === 'cheap'
      : requestedKeys.includes(counter.key) && counter.cost[windowKind] !== null
  })
}

export type CountRange = Extract<CountWindow, { kind: 'range' }>

const EPOCH = '1970-01-01T00:00:00.000Z'

export function boundedRange(window: CountWindow): CountRange | null {
  if (window.kind === 'range') {
    return window
  }
  return window.to === undefined ? null : { kind: 'range', from: EPOCH, to: window.to }
}

export function assertKnownCounterKeys(manifest: AnyManifest, keys: string[]): void {
  const known = new Set((manifest.counters ?? []).map((counter) => counter.key))
  const unknownKey = keys.find((key) => !known.has(key))
  if (unknownKey !== undefined) {
    throw new Error(`unknown counter key ${unknownKey} for ${manifest.platform}`)
  }
}
