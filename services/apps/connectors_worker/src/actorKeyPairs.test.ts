import { describe, expect, it } from 'vitest'

import { dropActorKeyPairs } from './actorKeyPairs'
import { IShadowDiffMismatch } from './shadowDiff'

const TS = '2026-10-16T10:41:43.000Z'
const LLVM_PR = 'PR_kwDOBITxeM8AAAABH9RDew'
const CHERRY_PR = 'PR_kwDOL_2xws8AAAABESoVOQ'

function mismatch(
  kind: IShadowDiffMismatch['kind'],
  sourceId: string,
  type = 'pull_request-merged',
): IShadowDiffMismatch {
  return { sourceId, type, kind, severity: 'high' }
}

describe('dropActorKeyPairs', () => {
  it('removes a nango undefined-actor merge event and its shadow twin from both sides', () => {
    const nangoOnly = mismatch('missing_in_shadow', `gen-ME_${LLVM_PR}_undefined_${TS}`)
    const shadowOnly = mismatch('missing_in_nango', `gen-ME_${LLVM_PR}_huntergr-arm_${TS}`)

    const result = dropActorKeyPairs([shadowOnly, nangoOnly])

    expect(result).toEqual({ mismatches: [], pairedCount: 1 })
  })

  it('removes a renamed-actor review pair even when the pull request node id contains underscores', () => {
    const nangoOnly = mismatch(
      'missing_in_shadow',
      `gen-PRR_${CHERRY_PR}_cherry-review_2026-10-17T00:09:35.000Z`,
      'pull_request-reviewed',
    )
    const shadowOnly = mismatch(
      'missing_in_nango',
      `gen-PRR_${CHERRY_PR}_cherry-pr-review_2026-10-17T00:09:35.000Z`,
      'pull_request-reviewed',
    )

    const result = dropActorKeyPairs([nangoOnly, shadowOnly])

    expect(result).toEqual({ mismatches: [], pairedCount: 1 })
  })

  it('pairs assignment events only when the assignee matches too', () => {
    const nangoOnly = mismatch(
      'missing_in_shadow',
      `gen-AE_${LLVM_PR}_undefined_alice_${TS}`,
      'pull_request-assigned',
    )
    const sameAssignee = mismatch(
      'missing_in_nango',
      `gen-AE_${LLVM_PR}_bob_alice_${TS}`,
      'pull_request-assigned',
    )
    const otherAssignee = mismatch(
      'missing_in_nango',
      `gen-AE_${LLVM_PR}_bob_carol_${TS}`,
      'pull_request-assigned',
    )

    expect(dropActorKeyPairs([otherAssignee, nangoOnly])).toEqual({
      mismatches: [otherAssignee, nangoOnly],
      pairedCount: 0,
    })
    expect(dropActorKeyPairs([otherAssignee, nangoOnly, sameAssignee])).toEqual({
      mismatches: [otherAssignee],
      pairedCount: 1,
    })
  })

  it('does not pair across different event prefixes or activity types', () => {
    const merged = mismatch('missing_in_shadow', `gen-ME_${LLVM_PR}_undefined_${TS}`)
    const closedSameType = mismatch('missing_in_nango', `gen-CE_${LLVM_PR}_alice_${TS}`)
    const mergedOtherType = mismatch(
      'missing_in_nango',
      `gen-ME_${LLVM_PR}_alice_${TS}`,
      'pull_request-closed',
    )

    const result = dropActorKeyPairs([merged, closedSameType, mergedOtherType])

    expect(result).toEqual({
      mismatches: [merged, closedSameType, mergedOtherType],
      pairedCount: 0,
    })
  })

  it('consumes each shadow-only mismatch at most once and keeps the output order', () => {
    const nangoOnly = mismatch('missing_in_shadow', `gen-ME_${LLVM_PR}_undefined_${TS}`)
    const firstTwin = mismatch('missing_in_nango', `gen-ME_${LLVM_PR}_alice_${TS}`)
    const secondTwin = mismatch('missing_in_nango', `gen-ME_${LLVM_PR}_bob_${TS}`)
    const rawId = mismatch('missing_in_nango', 'PRC_kwDOBITxeM4A', 'pull_request-comment')

    const result = dropActorKeyPairs([firstTwin, rawId, secondTwin, nangoOnly])

    expect(result).toEqual({ mismatches: [rawId, secondTwin], pairedCount: 1 })
  })

  it('leaves raw ids, field mismatches and malformed synthetic ids untouched', () => {
    const rawNangoOnly = mismatch('missing_in_shadow', 'PRC_kwDOBITxeM4A', 'pull_request-comment')
    const rawShadowOnly = mismatch('missing_in_nango', 'PRC_kwDOBITxeM4B', 'pull_request-comment')
    const fieldMismatch: IShadowDiffMismatch = {
      ...mismatch('field_mismatch', `gen-ME_${LLVM_PR}_alice_${TS}`),
      fields: [{ field: 'body', shadowValue: 'a', nangoValue: 'b' }],
    }
    const noParent = mismatch('missing_in_shadow', `gen-ME_undefined_${TS}`)
    const noParentTwin = mismatch('missing_in_nango', `gen-ME_alice_${TS}`)
    const unknownEvent = mismatch('missing_in_shadow', `gen-XX_${LLVM_PR}_undefined_${TS}`)
    const unknownEventTwin = mismatch('missing_in_nango', `gen-XX_${LLVM_PR}_alice_${TS}`)
    const input = [
      rawNangoOnly,
      rawShadowOnly,
      fieldMismatch,
      noParent,
      noParentTwin,
      unknownEvent,
      unknownEventTwin,
    ]

    const result = dropActorKeyPairs(input)

    expect(result).toEqual({ mismatches: input, pairedCount: 0 })
  })
})
