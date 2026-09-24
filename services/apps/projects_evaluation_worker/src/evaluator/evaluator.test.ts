import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { evaluateProject } from './evaluator'
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
    })
    expect(result).toEqual(apiResult)
  })

  it('returns an unsure/error result when the fetch call rejects', async () => {
    fetchMock.mockRejectedValue(new Error('network down'))

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('network down')
  })

  it('returns a rate-limited reason on HTTP 429 from the daily evaluation cap', async () => {
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

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toBe('rate limited: daily evaluation cap reached')
  })

  it('falls back to the generic reason on HTTP 429 from an unrelated rate limiter', async () => {
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

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('API returned HTTP 429')
  })

  it('falls back to the generic reason on HTTP 429 with a non-JSON body', async () => {
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

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('API returned HTTP 429')
  })

  it('returns an unsure/error result on a non-ok HTTP status', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    })

    const result = await evaluateProject(input)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('500')
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
