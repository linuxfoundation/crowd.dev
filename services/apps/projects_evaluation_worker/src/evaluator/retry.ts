export const TRANSIENT_EVALUATION_FAILURE = 'TransientEvaluationFailure'

export const MAX_EVALUATION_RUNS = 3

export const EVALUATION_ATTEMPTS = 3
export const EVALUATION_ATTEMPT_TIMEOUT_MINUTES = 3
export const EVALUATION_RETRY_INITIAL_SECONDS = 30
export const EVALUATION_RETRY_BACKOFF = 2
export const EVALUATION_REQUEST_TIMEOUT_MS = 150_000

export function evaluationWorstCaseMinutesPerProject(): number {
  let backoffSeconds = 0
  for (let attempt = 1; attempt < EVALUATION_ATTEMPTS; attempt++) {
    backoffSeconds += EVALUATION_RETRY_INITIAL_SECONDS * EVALUATION_RETRY_BACKOFF ** (attempt - 1)
  }
  return Math.ceil(EVALUATION_ATTEMPTS * EVALUATION_ATTEMPT_TIMEOUT_MINUTES + backoffSeconds / 60)
}

export function isTransientHttpStatus(status: number): boolean {
  return status === 429 || status >= 500
}
