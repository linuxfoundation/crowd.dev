const JITTER_RATIO = 0.1
const SHORT_DEFER_MIN_MS = 30_000
const SHORT_DEFER_JITTER_MS = 60_000
const FAILURE_BACKOFF_BASE_MS = 60_000
const FALLBACK_FAILURE_CAP_MS = 60 * 60_000
export const MIN_REQUESTED_DELAY_MS = 60_000
export const MAX_REQUESTED_DELAY_MS = 31 * 24 * 60 * 60_000

function withJitter(delayMs: number): number {
  return delayMs + delayMs * JITTER_RATIO * (Math.random() * 2 - 1)
}

export function cadenceRunAt(cadenceMinutes: number): Date {
  return new Date(Date.now() + withJitter(cadenceMinutes * 60_000))
}

export function shortDeferRunAt(): Date {
  return new Date(Date.now() + SHORT_DEFER_MIN_MS + Math.random() * SHORT_DEFER_JITTER_MS)
}

export function failureRunAt(consecutiveFailures: number, cadenceMinutes: number | null): Date {
  const capMs = cadenceMinutes === null ? FALLBACK_FAILURE_CAP_MS : cadenceMinutes * 60_000
  const backoffMs = FAILURE_BACKOFF_BASE_MS * 2 ** (Math.max(1, consecutiveFailures) - 1)

  return new Date(Date.now() + withJitter(Math.min(backoffMs, capMs)))
}

export function requestedRunAt(requested: Date, now: Date = new Date()): Date {
  const requestedMs = requested.getTime()
  if (Number.isNaN(requestedMs)) {
    throw new Error('requested next run is not a valid date')
  }
  const earliestMs = now.getTime() + MIN_REQUESTED_DELAY_MS
  const latestMs = now.getTime() + MAX_REQUESTED_DELAY_MS

  return new Date(Math.min(Math.max(requestedMs, earliestMs), latestMs))
}
