import type { BudgetProbe } from '../../pool/tokenPool'
import { PROBE_STALENESS_MS } from '../../pool/tokenPool'
import { CREDLY_MIN_CALL_INTERVAL_MS } from './apiClient'

export const CREDLY_BUDGET_WINDOW_MS = 60_000
export const CREDLY_SAFE_CALLS_PER_WINDOW = Math.floor(
  CREDLY_BUDGET_WINDOW_MS / CREDLY_MIN_CALL_INTERVAL_MS,
)

if (CREDLY_BUDGET_WINDOW_MS >= PROBE_STALENESS_MS) {
  throw new Error('credly budget window must stay under the token pool probe staleness window')
}

// Credly has no rate-limit endpoint or headers (linuxfoundation/lfx-cdp-insights-ops#864). Live ramp
// probe 2026-11-11: 125 calls at up to 32/min combined with a running worker, all 200, p90 ~2 s.
export const probeCredlyBudget: BudgetProbe = async () => ({
  limit: CREDLY_SAFE_CALLS_PER_WINDOW,
  remaining: CREDLY_SAFE_CALLS_PER_WINDOW,
  resetAt: new Date(Date.now() + CREDLY_BUDGET_WINDOW_MS),
})
