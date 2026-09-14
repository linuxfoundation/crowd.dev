import { IEvaluationInput, IEvaluationMetrics, IEvaluationResult } from './types'

interface IApiResponseContent {
  onboard: boolean
  non_onboard_reason?: string
}

interface IApiResponseMetrics {
  input_tokens: number
  output_tokens: number
  duration: number
}

export async function evaluateProject(input: IEvaluationInput): Promise<IEvaluationResult> {
  const endpoint = process.env.CROWD_PROJECT_EVALUATION_API_ENDPOINT
  const userId = process.env.CROWD_PROJECT_EVALUATION_API_USER_ID
  const secret = process.env.CROWD_PROJECT_EVALUATION_API_SECRET

  if (!endpoint || !userId || !secret) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason:
        'Missing API configuration: CROWD_PROJECT_EVALUATION_API_ENDPOINT, CROWD_PROJECT_EVALUATION_API_USER_ID, or CROWD_PROJECT_EVALUATION_API_SECRET',
      metrics: null,
    }
  }

  const body = new URLSearchParams()
  body.append('message', JSON.stringify({ repo_url: input.repoUrl }))
  body.append('stream', 'false')
  body.append('user_id', userId)

  let response: Response
  try {
    response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Bearer ${secret}`,
      },
      body,
    })
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `API request failed: ${message}`,
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

  let responseBody: unknown
  try {
    responseBody = await response.json()
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `Failed to parse API response: ${message}`,
      metrics: null,
    }
  }

  const content = (responseBody as { content?: unknown } | null)?.content
  if (
    !content ||
    typeof content !== 'object' ||
    typeof (content as IApiResponseContent).onboard !== 'boolean'
  ) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `Unexpected API response shape: ${JSON.stringify(responseBody)}`,
      metrics: null,
    }
  }

  const { onboard, non_onboard_reason } = content as IApiResponseContent

  return {
    outcome: onboard ? 'onboard' : 'skip',
    evaluationResult: String(onboard),
    evaluationReason: non_onboard_reason ?? null,
    metrics: parseMetrics(responseBody),
  }
}

function parseMetrics(responseBody: unknown): IEvaluationMetrics | null {
  const { model, metrics } = (responseBody ?? {}) as { model?: unknown; metrics?: unknown }

  if (
    typeof model !== 'string' ||
    !metrics ||
    typeof metrics !== 'object' ||
    typeof (metrics as IApiResponseMetrics).input_tokens !== 'number' ||
    typeof (metrics as IApiResponseMetrics).output_tokens !== 'number' ||
    typeof (metrics as IApiResponseMetrics).duration !== 'number'
  ) {
    return null
  }

  const { input_tokens, output_tokens, duration } = metrics as IApiResponseMetrics

  return { model, inputTokens: input_tokens, outputTokens: output_tokens, seconds: duration }
}
