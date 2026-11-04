import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { fetchScorecard } from '../scorecardApi'

const cfg = { apiBaseUrl: 'https://scorecard.test', fetchTimeoutMs: 5000 }

function fakeResponse(
  status: number,
  body?: unknown,
  headers: Record<string, string> = {},
): Response {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
    json: async () => {
      if (body === undefined) throw new SyntaxError('Unexpected end of JSON input')
      return body
    },
  } as unknown as Response
}

const okBody = {
  date: '2026-10-19',
  repo: { name: 'github.com/kubernetes/kubernetes', commit: 'abc' },
  score: 7.6,
  checks: [{ name: 'Code-Review', score: 10, reason: 'all changesets reviewed', details: null }],
}

async function withTimers<T>(run: () => Promise<T>): Promise<T> {
  const pending = run()
  await vi.runAllTimersAsync()
  return pending
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('fetchScorecard', () => {
  it('requests the project path verbatim and parses the payload', async () => {
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse(200, okBody))
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchScorecard('github.com/chartjs/Chart.js', cfg)

    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://scorecard.test/projects/github.com/chartjs/Chart.js',
    )
    expect(result).toEqual({
      kind: 'OK',
      payload: {
        date: '2026-10-19',
        score: 7.6,
        checks: [{ name: 'Code-Review', score: 10, reason: 'all changesets reviewed' }],
      },
    })
  })

  it('maps an empty-body 404 to NOT_FOUND', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(404)))
    expect(await fetchScorecard('github.com/a/b', cfg)).toEqual({ kind: 'NOT_FOUND' })
  })

  it('retries a 429 honouring Retry-After', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(fakeResponse(429, undefined, { 'retry-after': '1' }))
      .mockResolvedValueOnce(fakeResponse(200, okBody))
    vi.stubGlobal('fetch', fetchMock)

    const result = await withTimers(() => fetchScorecard('github.com/a/b', cfg))

    expect(result.kind).toBe('OK')
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('gives up on persistent 429 with RATE_LIMIT', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(429)))
    const result = await withTimers(() => fetchScorecard('github.com/a/b', cfg))
    expect(result).toMatchObject({ kind: 'RATE_LIMIT', statusCode: 429 })
  })

  it('retries a 5xx and then succeeds', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(fakeResponse(502))
      .mockResolvedValueOnce(fakeResponse(200, okBody))
    vi.stubGlobal('fetch', fetchMock)

    const result = await withTimers(() => fetchScorecard('github.com/a/b', cfg))

    expect(result.kind).toBe('OK')
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('maps a network failure to TRANSIENT after retries', async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error('ECONNRESET'))
    vi.stubGlobal('fetch', fetchMock)

    const result = await withTimers(() => fetchScorecard('github.com/a/b', cfg))

    expect(result).toMatchObject({ kind: 'TRANSIENT', message: 'network error: ECONNRESET' })
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('aborts a stalled body read at the timeout and retries it as transient', async () => {
    const stalled = {
      ...fakeResponse(200),
      json: () =>
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('aborted')), cfg.fetchTimeoutMs + 1),
        ),
    } as unknown as Response
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(stalled)
      .mockResolvedValueOnce(fakeResponse(200, okBody))
    vi.stubGlobal('fetch', fetchMock)

    const result = await withTimers(() => fetchScorecard('github.com/a/b', cfg))

    expect(result.kind).toBe('OK')
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('caps the Retry-After wait and reports each retry wait', async () => {
    const onRetryWait = vi.fn()
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(fakeResponse(429, undefined, { 'retry-after': '3600' }))
      .mockResolvedValueOnce(fakeResponse(200, okBody))
    vi.stubGlobal('fetch', fetchMock)

    const pending = fetchScorecard('github.com/a/b', { ...cfg, onRetryWait })
    await vi.advanceTimersByTimeAsync(60_000)

    expect((await pending).kind).toBe('OK')
    expect(onRetryWait).toHaveBeenCalledTimes(1)
  })

  it('does not treat other 4xx as not scanned', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(403)))
    expect(await fetchScorecard('github.com/a/b', cfg)).toMatchObject({
      kind: 'TRANSIENT',
      statusCode: 403,
    })
  })

  it('maps invalid json and unexpected shapes to MALFORMED', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200)))
    expect(await fetchScorecard('github.com/a/b', cfg)).toMatchObject({ kind: 'MALFORMED' })

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, { score: 'high' })))
    expect(await fetchScorecard('github.com/a/b', cfg)).toMatchObject({ kind: 'MALFORMED' })
  })
})
