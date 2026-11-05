import { describe, expect, it } from 'vitest'

import { canDeferEvaluation } from './evaluationRetry'

describe('canDeferEvaluation', () => {
  it('lets a scheduled row wait for the next run until the last allowed run', () => {
    expect(canDeferEvaluation({ provenance: 'lf-criticality-score', evaluationAttempts: 0 })).toBe(
      true,
    )
    expect(canDeferEvaluation({ provenance: null, evaluationAttempts: 1 })).toBe(true)
    expect(canDeferEvaluation({ provenance: 'lf-criticality-score', evaluationAttempts: 2 })).toBe(
      false,
    )
  })

  it('never defers a bot request, which gets the Try again button instead', () => {
    expect(canDeferEvaluation({ provenance: 'slack-bot', evaluationAttempts: 0 })).toBe(false)
  })
})
