import { describe, expect, test } from 'vitest'

import { LlmModelType } from './enums/llm'
import { estimateLlmCostUsd } from './llm'

describe('estimateLlmCostUsd', () => {
  test('computes cost from input/output tokens for a known model', () => {
    const cost = estimateLlmCostUsd(LlmModelType.CLAUDE_SONNET_4, 21713, 907)

    expect(cost).toBeCloseTo(0.078753, 6)
  })

  test('returns null for an unknown model id', () => {
    const cost = estimateLlmCostUsd('some-future-model-id', 1000, 1000)

    expect(cost).toBeNull()
  })
})
