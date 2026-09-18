import { getErrorMessage } from '@crowd/common'

import { IEvaluationInput, IEvaluationMetrics, IEvaluationResult } from './types'

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
    })
  } catch (err) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `API request failed: ${getErrorMessage(err)}`,
      metrics: null,
    }
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
