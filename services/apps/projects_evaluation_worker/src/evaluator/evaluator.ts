import { ApplicationFailure } from '@temporalio/activity'

import { getErrorMessage } from '@crowd/common'

import {
  EVALUATION_REQUEST_TIMEOUT_MS,
  TRANSIENT_EVALUATION_FAILURE,
  isTransientHttpStatus,
} from './retry'
import { IEvaluationInput, IEvaluationMetrics, IEvaluationResult } from './types'

export function transientEvaluationFailure(
  message: string,
  { retryNow = true }: { retryNow?: boolean } = {},
): ApplicationFailure {
  return ApplicationFailure.create({
    type: TRANSIENT_EVALUATION_FAILURE,
    message,
    nonRetryable: !retryNow,
  })
}

function isValidMetrics(metrics: unknown): metrics is IEvaluationMetrics | null {
  if (metrics === null) {
    return true
  }
  const candidate = metrics as Partial<IEvaluationMetrics>
  return (
    typeof candidate === 'object' &&
    typeof candidate.model === 'string' &&
    typeof candidate.inputTokens === 'number' &&
    typeof candidate.outputTokens === 'number' &&
    typeof candidate.seconds === 'number'
  )
}

function isValidResult(result: unknown): result is IEvaluationResult {
  const candidate = result as Partial<IEvaluationResult> | null
  return (
    !!candidate &&
    typeof candidate === 'object' &&
    ['onboard', 'skip', 'unsure'].includes(candidate.outcome) &&
    typeof candidate.evaluationResult === 'string' &&
    (candidate.evaluationReason === null || typeof candidate.evaluationReason === 'string') &&
    isValidMetrics(candidate.metrics)
  )
}

function isAbortError(err: unknown): boolean {
  return err instanceof Error && (err.name === 'AbortError' || err.name === 'TimeoutError')
}

async function responseErrorMessage(response: Response): Promise<string> {
  try {
    const body = (await response.clone().json()) as { error?: { message?: string } }
    return body?.error?.message ?? response.statusText
  } catch {
    return response.statusText
  }
}

export async function evaluateProject(input: IEvaluationInput): Promise<IEvaluationResult> {
  const apiUrl = process.env.CROWD_API_SERVICE_URL
  const apiKey = process.env.CROWD_PROJECT_EVALUATION_STATIC_API_KEY

  if (!apiUrl || !apiKey) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason:
        'Missing API configuration: CROWD_API_SERVICE_URL or CROWD_PROJECT_EVALUATION_STATIC_API_KEY',
      metrics: null,
    }
  }

  let response: Response
  try {
    response = await fetch(`${apiUrl}/v1/project-evaluation`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(input),
      signal: AbortSignal.timeout(EVALUATION_REQUEST_TIMEOUT_MS),
    })
  } catch (err) {
    throw transientEvaluationFailure(`API request failed: ${getErrorMessage(err)}`)
  }

  if (response.status === 429) {
    let dailyCapReached = false
    try {
      const body = (await response.clone().json()) as { error?: { message?: string } }
      dailyCapReached = body?.error?.message === 'Daily evaluation limit reached'
    } catch {
      dailyCapReached = false
    }

    if (dailyCapReached) {
      throw transientEvaluationFailure('rate limited: daily evaluation cap reached', {
        retryNow: false,
      })
    }
  }

  if (isTransientHttpStatus(response.status)) {
    throw transientEvaluationFailure(
      `API returned HTTP ${response.status}: ${await responseErrorMessage(response)}`,
    )
  }

  if (!response.ok) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `API returned HTTP ${response.status}: ${response.statusText}`,
      metrics: null,
    }
  }

  let result: unknown
  try {
    result = await response.json()
  } catch (err) {
    if (isAbortError(err)) {
      throw transientEvaluationFailure(`API response stalled: ${getErrorMessage(err)}`)
    }
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `Failed to parse API response: ${getErrorMessage(err)}`,
      metrics: null,
    }
  }

  if (!isValidResult(result)) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `Unexpected API response shape: ${JSON.stringify(result)}`,
      metrics: null,
    }
  }

  return result
}
