import { IShadowDiffMismatch } from './shadowDiff'

const SYNTHETIC_SOURCE_ID_PREFIX = 'gen-'
const SEGMENT_SEPARATOR = '_'
const TRAILING_SEGMENTS_BY_EVENT: Record<string, number> = {
  AE: 3,
  RRE: 3,
  PRR: 2,
  ME: 2,
  CE: 2,
}

export interface ActorKeyPairFilterResult {
  mismatches: IShadowDiffMismatch[]
  pairedCount: number
}

function actorKeyPairingKey(mismatch: IShadowDiffMismatch): string | null {
  if (!mismatch.sourceId.startsWith(SYNTHETIC_SOURCE_ID_PREFIX)) {
    return null
  }
  const body = mismatch.sourceId.slice(SYNTHETIC_SOURCE_ID_PREFIX.length)
  const eventEnd = body.indexOf(SEGMENT_SEPARATOR)
  if (eventEnd === -1) {
    return null
  }
  const event = body.slice(0, eventEnd)
  const trailingSegments = TRAILING_SEGMENTS_BY_EVENT[event]
  if (trailingSegments === undefined) {
    return null
  }
  const segments = body.slice(eventEnd + 1).split(SEGMENT_SEPARATOR)
  if (segments.length <= trailingSegments) {
    return null
  }
  const parentId = segments.slice(0, segments.length - trailingSegments).join(SEGMENT_SEPARATOR)
  const timestamp = segments[segments.length - 1]
  const target = trailingSegments === 3 ? segments[segments.length - 2] : ''
  return [mismatch.type, event, parentId, target, timestamp].join('|')
}

export function dropActorKeyPairs(mismatches: IShadowDiffMismatch[]): ActorKeyPairFilterResult {
  const shadowOnlyIndexesByKey = new Map<string, number[]>()
  mismatches.forEach((mismatch, index) => {
    if (mismatch.kind !== 'missing_in_nango') {
      return
    }
    const key = actorKeyPairingKey(mismatch)
    if (key === null) {
      return
    }
    const indexes = shadowOnlyIndexesByKey.get(key) ?? []
    indexes.push(index)
    shadowOnlyIndexesByKey.set(key, indexes)
  })

  const pairedIndexes = new Set<number>()
  mismatches.forEach((mismatch, index) => {
    if (mismatch.kind !== 'missing_in_shadow') {
      return
    }
    const key = actorKeyPairingKey(mismatch)
    if (key === null) {
      return
    }
    const twinIndex = shadowOnlyIndexesByKey.get(key)?.shift()
    if (twinIndex === undefined) {
      return
    }
    pairedIndexes.add(index)
    pairedIndexes.add(twinIndex)
  })

  return {
    mismatches: mismatches.filter((_, index) => !pairedIndexes.has(index)),
    pairedCount: pairedIndexes.size / 2,
  }
}
