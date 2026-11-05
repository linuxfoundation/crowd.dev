import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { evaluateProject } from './evaluator'
import { TRANSIENT_EVALUATION_FAILURE } from './retry'
import { IEvaluationInput } from './types'

const input: IEvaluationInput = {
  id: 'catalog-1',
  repoUrl: 'https://github.com/foo/bar',
  repoName: 'bar',
  projectSlug: 'foo',
  lfCriticalityScore: null,
  source: null,
}

describe('evaluateProject', () => {
  const originalEnv = { ...process.env }
  const fetchMock = vi.fn()

  beforeEach(() => {
    process.env.CROWD_API_SERVICE_URL = 'https://api.example.com'
    process.env.CROWD_PROJECT_EVALUATION_STATIC_API_KEY = 'test-key'
    vi.stubGlobal('fetch', fetchMock)
    fetchMock.mockReset()
  })

  afterEach(() => {
    process.env = { ...originalEnv }
    vi.unstubAllGlobals()
  })

  it('returns an unsure/error result when required env vars are missing', async () => {
    delete process.env.CROWD_API_SERVICE_URL

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationResult).toBe('error')
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('calls the internal project-evaluation endpoint and returns its result', async () => {
    const apiResult = {
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: { model: 'test-model', inputTokens: 10, outputTokens: 5, seconds: 1 },
    }
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => apiResult,
    })

    const result = await evaluateProject(input)

    expect(fetchMock).toHaveBeenCalledWith('https://api.example.com/v1/project-evaluation', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: 'Bearer test-key',
      },
      body: JSON.stringify(input),
      signal: expect.any(AbortSignal),
    })
    expect(result).toEqual(apiResult)
  })

  it('throws a retryable transient failure when the fetch call rejects', async () => {
    fetchMock.mockRejectedValue(new Error('network down'))

    await expect(evaluateProject(input)).rejects.toMatchObject({
      type: TRANSIENT_EVALUATION_FAILURE,
      nonRetryable: false,
      message: 'API request failed: network down',
    })
  })

  it('throws a transient failure without an immediate retry on the daily evaluation cap', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 429,
      statusText: 'Too Many Requests',
      clone: () => ({
        json: async () => ({
          error: { code: 'RATE_LIMITED', message: 'Daily evaluation limit reached' },
        }),
      }),
    })

    await expect(evaluateProject(input)).rejects.toMatchObject({
      type: TRANSIENT_EVALUATION_FAILURE,
      nonRetryable: true,
      message: 'rate limited: daily evaluation cap reached',
    })
  })

  it('throws a retryable transient failure on HTTP 429 from an unrelated rate limiter', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 429,
      statusText: 'Too Many Requests',
      clone: () => ({
        json: async () => ({
          error: { code: 'RATE_LIMITED', message: 'Too many requests, please try again later' },
        }),
      }),
    })

    await expect(evaluateProject(input)).rejects.toMatchObject({
      type: TRANSIENT_EVALUATION_FAILURE,
      nonRetryable: false,
      message: 'API returned HTTP 429: Too many requests, please try again later',
    })
  })

  it('throws a transient failure on HTTP 429 with a non-JSON body', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 429,
      statusText: 'Too Many Requests',
      clone: () => ({
        json: async () => {
          throw new SyntaxError('Unexpected token')
        },
      }),
    })

    await expect(evaluateProject(input)).rejects.toMatchObject({
      type: TRANSIENT_EVALUATION_FAILURE,
      message: 'API returned HTTP 429: Too Many Requests',
    })
  })

  it('throws a transient failure on a server error, with the reason the API gave', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 503,
      statusText: 'Service Unavailable',
      clone: () => ({
        json: async () => ({
          error: { code: 'SERVICE_UNAVAILABLE', message: 'Bedrock throttled' },
        }),
      }),
    })

    await expect(evaluateProject(input)).rejects.toMatchObject({
      type: TRANSIENT_EVALUATION_FAILURE,
      nonRetryable: false,
      message: 'API returned HTTP 503: Bedrock throttled',
    })
  })

  it('returns an unsure/error result on a client error that a retry would not fix', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
    })

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationResult).toBe('error')
    expect(result.evaluationReason).toContain('400')
  })

  it('throws a transient failure when the response body stalls past the deadline', async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => {
        throw Object.assign(new Error('The operation was aborted due to timeout'), {
          name: 'TimeoutError',
        })
      },
    })

    await expect(evaluateProject(input)).rejects.toMatchObject({
      type: TRANSIENT_EVALUATION_FAILURE,
      nonRetryable: false,
      message: 'API response stalled: The operation was aborted due to timeout',
    })
  })

  it('returns an unsure/error result when the response body is not valid JSON', async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => {
        throw new SyntaxError('Unexpected token')
      },
    })

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('Unexpected token')
  })

  it('returns an unsure/error result when the response has an unexpected outcome value', async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ outcome: 'maybe' }),
    })

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('Unexpected API response shape')
  })

  it('returns an unsure/error result when a valid outcome is paired with malformed metrics', async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        outcome: 'onboard',
        evaluationResult: 'true',
        evaluationReason: null,
        metrics: { model: 123, inputTokens: '5', outputTokens: 5, seconds: 1 },
      }),
    })

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('Unexpected API response shape')
  })

  it('returns an unsure/error result when a valid outcome is missing evaluationResult', async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ outcome: 'onboard', evaluationReason: null, metrics: null }),
    })

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('Unexpected API response shape')
  })
})
