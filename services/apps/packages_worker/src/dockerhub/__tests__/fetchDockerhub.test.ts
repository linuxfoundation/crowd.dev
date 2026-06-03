import { afterEach, describe, expect, it, vi } from 'vitest'

import { fetchDockerhub } from '../fetchDockerhub'
import { FetchError } from '../types'

const BASE = 'https://hub.docker.com/v2'

function mockFetch(status: number, body: unknown, headers: Record<string, string> = {}) {
  return vi.spyOn(globalThis, 'fetch').mockResolvedValue(
    new Response(typeof body === 'string' ? body : JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json', ...headers },
    }),
  )
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('fetchDockerhub', () => {
  it('returns pull/star counts on 200', async () => {
    mockFetch(200, {
      name: 'grafana',
      namespace: 'grafana',
      pull_count: 12345,
      star_count: 678,
      last_updated: '2026-05-01T00:00:00Z',
    })

    const r = await fetchDockerhub(BASE, 'grafana/grafana')
    expect(r).toEqual({
      imageName: 'grafana/grafana',
      pulls: 12345,
      stars: 678,
      lastUpdated: '2026-05-01T00:00:00Z',
    })
  })

  it('appends trailing slash to the request URL', async () => {
    const spy = mockFetch(200, { pull_count: 1, star_count: 0 })
    await fetchDockerhub(BASE, 'a/b')
    expect(spy).toHaveBeenCalledWith(`${BASE}/repositories/a/b/`, expect.anything())
  })

  it('classifies 404 as NOT_FOUND', async () => {
    mockFetch(404, { message: 'object not found' })
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'NOT_FOUND' })
  })

  it('classifies 400 as NOT_FOUND (Hub 400s on malformed slugs)', async () => {
    mockFetch(400, { message: 'bad request' })
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'NOT_FOUND' })
  })

  it('classifies 429 as RATE_LIMIT with resetAt from header', async () => {
    const resetSec = Math.floor(Date.now() / 1000) + 120
    mockFetch(429, { message: 'too many' }, { 'x-ratelimit-reset': String(resetSec) })

    expect.assertions(3)
    try {
      await fetchDockerhub(BASE, 'a/b')
    } catch (err) {
      expect(err).toBeInstanceOf(FetchError)
      const fe = err as FetchError
      expect(fe.kind).toBe('RATE_LIMIT')
      expect(fe.resetAt).toBeGreaterThan(Date.now())
    }
  })

  it('does NOT discard a 200 response when x-ratelimit-remaining is 0', async () => {
    // remaining=0 means this request consumed the last slot; the response itself
    // is valid. The next call will 429 and park then.
    mockFetch(200, { pull_count: 1, star_count: 0 }, { 'x-ratelimit-remaining': '0' })
    const r = await fetchDockerhub(BASE, 'a/b')
    expect(r.pulls).toBe(1)
  })

  it('classifies 401/403 as AUTH so misconfig surfaces instead of looking like a miss', async () => {
    mockFetch(401, { message: 'unauthorized' })
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'AUTH' })
    mockFetch(403, { message: 'forbidden' })
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'AUTH' })
  })

  it('classifies 5xx as TRANSIENT', async () => {
    mockFetch(503, 'Service Unavailable')
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'TRANSIENT' })
  })

  it('classifies network failure as TRANSIENT', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('ECONNRESET'))
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'TRANSIENT' })
  })

  it('classifies non-JSON 200 body as MALFORMED', async () => {
    mockFetch(200, '<html>not json</html>')
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'MALFORMED' })
  })

  it('classifies missing pull_count as MALFORMED', async () => {
    mockFetch(200, { name: 'x', star_count: 0 })
    await expect(fetchDockerhub(BASE, 'a/b')).rejects.toMatchObject({ kind: 'MALFORMED' })
  })
})
