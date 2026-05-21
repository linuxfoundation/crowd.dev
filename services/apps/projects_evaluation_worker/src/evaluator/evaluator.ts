import { IEvaluationInput, IEvaluationResult } from './types'

interface IApiResponseContent {
  onboard: boolean
  non_onboard_reason?: string
}

interface IApiResponse {
  content: IApiResponseContent
}

export async function evaluateProject(input: IEvaluationInput): Promise<IEvaluationResult> {
  const endpoint = process.env.CROWD_PROJECT_EVALUATION_API_ENDPOINT
  const userId = process.env.CROWD_PROJECT_EVALUATION_API_USER_ID
  const secret = process.env.CROWD_PROJECT_EVALUATION_API_SECRET

  if (!endpoint || !userId || !secret) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: 'Missing API configuration: CROWD_PROJECT_EVALUATION_API_ENDPOINT, CROWD_PROJECT_EVALUATION_API_USER_ID, or CROWD_PROJECT_EVALUATION_API_SECRET',
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
    }
  }

  if (!response.ok) {
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `API returned HTTP ${response.status}: ${response.statusText}`,
    }
  }

  let responseBody: IApiResponse
  try {
    responseBody = (await response.json()) as IApiResponse
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return {
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: `Failed to parse API response: ${message}`,
    }
  }

  const { onboard, non_onboard_reason } = responseBody.content

  return {
    outcome: onboard ? 'onboard' : 'skip',
    evaluationResult: String(onboard),
    evaluationReason: non_onboard_reason ?? '',
  }
}
