import { afterEach, describe, expect, it, vi } from 'vitest'

import { fetchVersionList } from '../proxyClient'

function fakeResponse(
  status: number,
  text?: string,
  headers: Record<string, string> = {},
): Response {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
    text: async () => text ?? '',
  } as unknown as Response
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('fetchVersionList', () => {
  it('parses newline-separated versions, trimming blanks', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(fakeResponse(200, 'v1.0.0\nv1.1.0\n\nv2.0.0\n')),
    )
    const result = await fetchVersionList('github.com/pubnub/go/v7', 5000)
    expect(result).toEqual(['v1.0.0', 'v1.1.0', 'v2.0.0'])
  })

  it('maps a 404 to NOT_FOUND', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(404)))
    const result = await fetchVersionList('nope/nope', 5000)
    expect(result).toMatchObject({ kind: 'NOT_FOUND', statusCode: 404 })
  })

  it('retries on 429 with Retry-After and eventually succeeds', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(fakeResponse(429, undefined, { 'retry-after': '0' }))
      .mockResolvedValueOnce(fakeResponse(200, 'v1.0.0\n'))
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchVersionList('github.com/pubnub/go/v7', 5000)
    expect(result).toEqual(['v1.0.0'])
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('maps a network rejection to TRANSIENT', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('ECONNRESET')))
    const result = await fetchVersionList('github.com/pubnub/go/v7', 5000)
    expect(result).toMatchObject({ kind: 'TRANSIENT' })
  })
})
