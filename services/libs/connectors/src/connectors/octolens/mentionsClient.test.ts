import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ProviderContractError, RateLimitError } from '../../http/errors'
import { fetchMentionsPage } from './mentionsClient'

vi.mock('axios')
vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  timeout: vi.fn().mockResolvedValue(undefined),
}))

const mockedPost = vi.mocked(axios.post)

beforeEach(() => {
  mockedPost.mockReset()
})

describe('fetchMentionsPage', () => {
  it('sends the bearer token and returns the data/nextCursor from a successful response', async () => {
    mockedPost.mockResolvedValue({
      status: 200,
      headers: {},
      data: { data: [{ id: 1 }], pagination: { nextCursor: 'abc' } },
    })

    const page = await fetchMentionsPage('test-token', { startDate: '2026-10-01T00:00:00.000Z' })

    expect(page).toEqual({ data: [{ id: 1 }], nextCursor: 'abc' })
    expect(mockedPost).toHaveBeenCalledWith(
      'https://app.octolens.com/api/v2/mentions',
      expect.objectContaining({
        filters: { startDate: '2026-10-01T00:00:00.000Z' },
      }),
      expect.objectContaining({
        headers: { Authorization: 'Bearer test-token' },
      }),
    )
  })

  it('asks for relevant mentions only, with their engagement metrics', async () => {
    mockedPost.mockResolvedValue({
      status: 200,
      headers: {},
      data: { data: [], pagination: { nextCursor: null } },
    })

    await fetchMentionsPage('test-token', {})

    expect(mockedPost).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ includeEngagementMetrics: true }),
      expect.anything(),
    )
    expect(mockedPost.mock.calls[0][1]).not.toHaveProperty('includeAll')
  })

  it('omits filters when no startDate is given', async () => {
    mockedPost.mockResolvedValue({
      status: 200,
      headers: {},
      data: { data: [], pagination: { nextCursor: null } },
    })

    await fetchMentionsPage('test-token', {})

    expect(mockedPost).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ filters: undefined }),
      expect.anything(),
    )
  })

  it('filters by keyword ids, alone or together with startDate', async () => {
    mockedPost.mockResolvedValue({
      status: 200,
      headers: {},
      data: { data: [], pagination: { nextCursor: null } },
    })

    await fetchMentionsPage('test-token', { keywordIds: [7] })
    await fetchMentionsPage('test-token', { keywordIds: [7], startDate: '2026-10-01T00:00:00Z' })

    expect(mockedPost.mock.calls.map(([, body]) => (body as { filters: unknown }).filters)).toEqual(
      [{ keyword: [7] }, { startDate: '2026-10-01T00:00:00Z', keyword: [7] }],
    )
  })

  it('throws a RateLimitError with resumeAt from Retry-After on a 429, with no retry', async () => {
    mockedPost.mockResolvedValue({
      status: 429,
      headers: { 'retry-after': '30' },
      data: { code: 'RATE_LIMITED' },
    })

    const before = Date.now()
    await expect(fetchMentionsPage('test-token', {})).rejects.toSatisfy((err: RateLimitError) => {
      expect(err).toBeInstanceOf(RateLimitError)
      expect(err.options?.resumeAt).toBeInstanceOf(Date)
      expect(err.options.resumeAt.getTime()).toBeGreaterThanOrEqual(before + 29_000)
      return true
    })
    expect(mockedPost).toHaveBeenCalledTimes(1)
  })

  it('falls back to X-RateLimit-Reset when Retry-After is absent', async () => {
    const resetEpochSeconds = Math.floor(Date.now() / 1000) + 120
    mockedPost.mockResolvedValue({
      status: 429,
      headers: { 'x-ratelimit-reset': String(resetEpochSeconds) },
      data: {},
    })

    await expect(fetchMentionsPage('test-token', {})).rejects.toSatisfy((err: RateLimitError) => {
      expect(err.options?.resumeAt?.getTime()).toBe(resetEpochSeconds * 1000)
      return true
    })
  })

  it('retries a 5xx failure and succeeds once a later attempt returns 200', async () => {
    mockedPost.mockResolvedValueOnce({ status: 503, headers: {}, data: {} }).mockResolvedValueOnce({
      status: 200,
      headers: {},
      data: { data: [], pagination: { nextCursor: null } },
    })

    const page = await fetchMentionsPage('test-token', {})

    expect(page).toEqual({ data: [], nextCursor: null })
    expect(mockedPost).toHaveBeenCalledTimes(2)
  })

  it('gives up after exhausting retries on a persistent 5xx failure', async () => {
    mockedPost.mockResolvedValue({ status: 500, headers: {}, data: {} })

    await expect(fetchMentionsPage('test-token', {})).rejects.toThrow()
    expect(mockedPost).toHaveBeenCalledTimes(4)
  })

  it('does not retry a 4xx contract error', async () => {
    mockedPost.mockResolvedValue({ status: 400, headers: {}, data: {} })

    await expect(fetchMentionsPage('test-token', {})).rejects.toThrow()
    expect(mockedPost).toHaveBeenCalledTimes(1)
  })

  it('throws a contract error when a 2xx response is missing data/pagination', async () => {
    mockedPost.mockResolvedValue({ status: 200, headers: {}, data: {} })

    await expect(fetchMentionsPage('test-token', {})).rejects.toBeInstanceOf(ProviderContractError)
  })

  it('throws a contract error when nextCursor is omitted instead of explicitly null', async () => {
    mockedPost.mockResolvedValue({
      status: 200,
      headers: {},
      data: { data: [], pagination: {} },
    })

    await expect(fetchMentionsPage('test-token', {})).rejects.toBeInstanceOf(ProviderContractError)
  })

  it('throws a contract error when nextCursor is an empty string', async () => {
    mockedPost.mockResolvedValue({
      status: 200,
      headers: {},
      data: { data: [], pagination: { nextCursor: '' } },
    })

    await expect(fetchMentionsPage('test-token', {})).rejects.toBeInstanceOf(ProviderContractError)
  })

  it('retries a raw transport failure (e.g. connection refused) like a 5xx response', async () => {
    mockedPost.mockRejectedValueOnce(new Error('ECONNREFUSED')).mockResolvedValueOnce({
      status: 200,
      headers: {},
      data: { data: [], pagination: { nextCursor: null } },
    })

    const page = await fetchMentionsPage('test-token', {})

    expect(page).toEqual({ data: [], nextCursor: null })
    expect(mockedPost).toHaveBeenCalledTimes(2)
  })

  it('gives up after exhausting retries on a persistent transport failure', async () => {
    mockedPost.mockRejectedValue(new Error('ETIMEDOUT'))

    await expect(fetchMentionsPage('test-token', {})).rejects.toThrow()
    expect(mockedPost).toHaveBeenCalledTimes(4)
  })
})
