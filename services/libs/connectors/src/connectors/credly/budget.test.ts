import { describe, expect, it } from 'vitest'

import { PROBE_STALENESS_MS } from '../../pool/tokenPool'
import { CREDLY_BUDGET_WINDOW_MS, CREDLY_SAFE_CALLS_PER_WINDOW, probeCredlyBudget } from './budget'

describe('probeCredlyBudget', () => {
  it('returns a fresh full snapshot sized to the known-safe call rate', async () => {
    const before = Date.now()
    const snapshot = await probeCredlyBudget('credly', { id: 'credly-org', value: 'token' })
    const after = Date.now()

    expect(snapshot).not.toBeNull()
    expect(snapshot!.limit).toBe(CREDLY_SAFE_CALLS_PER_WINDOW)
    expect(snapshot!.remaining).toBe(CREDLY_SAFE_CALLS_PER_WINDOW)
    expect(snapshot!.resetAt.getTime()).toBeGreaterThanOrEqual(before + CREDLY_BUDGET_WINDOW_MS)
    expect(snapshot!.resetAt.getTime()).toBeLessThanOrEqual(after + CREDLY_BUDGET_WINDOW_MS)
  })

  it('keeps its window under the pool probe staleness, so resetAt always re-probes first', () => {
    expect(CREDLY_BUDGET_WINDOW_MS).toBeLessThan(PROBE_STALENESS_MS)
  })
})
