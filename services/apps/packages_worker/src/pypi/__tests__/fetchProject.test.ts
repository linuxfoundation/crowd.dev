import { afterEach, describe, expect, it, vi } from 'vitest'

import { fetchProject } from '../fetchProject'

// Minimal Response stand-in for the global fetch mock.
function fakeResponse(status: number, body?: unknown, jsonThrows = false): Response {
  return {
    status,
    ok: status >= 200 && status < 300,
    json: async () => {
      if (jsonThrows) throw new Error('bad json')
      return body
    },
  } as unknown as Response
}

const validProject = { info: { name: 'flask' }, releases: {} }

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('fetchProject status → FetchError kind mapping', () => {
  it('returns the parsed project on 200 with a valid shape', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, validProject)))
    const result = await fetchProject('flask')
    expect(result).toEqual(validProject)
  })

  it('maps 404 → NOT_FOUND (skippable)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(404)))
    expect(await fetchProject('nope')).toMatchObject({ kind: 'NOT_FOUND', statusCode: 404 })
  })

  it('maps 429 → RATE_LIMIT (transient, distinct from generic 4xx)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(429)))
    expect(await fetchProject('busy')).toMatchObject({ kind: 'RATE_LIMIT', statusCode: 429 })
  })

  it('maps other non-ok statuses (5xx) → TRANSIENT with the status code', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(503)))
    expect(await fetchProject('flaky')).toMatchObject({ kind: 'TRANSIENT', statusCode: 503 })
  })

  it('maps a network/fetch rejection → TRANSIENT (no status code)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('ECONNRESET')))
    const result = await fetchProject('flask')
    expect(result).toMatchObject({ kind: 'TRANSIENT' })
    expect((result as { statusCode?: number }).statusCode).toBeUndefined()
  })

  it('maps an unparseable body → MALFORMED', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, undefined, true)))
    expect(await fetchProject('flask')).toMatchObject({ kind: 'MALFORMED' })
  })

  it('maps an unexpected JSON shape → MALFORMED', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, { not: 'a project' })))
    expect(await fetchProject('flask')).toMatchObject({ kind: 'MALFORMED' })
  })

  it('maps a body read aborted by the 30s timeout → TRANSIENT (not MALFORMED)', async () => {
    vi.useFakeTimers()
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        status: 200,
        ok: true,
        // body never settles before the 30s abort fires, then rejects (as an aborted read would)
        json: () =>
          new Promise((_resolve, reject) => setTimeout(() => reject(new Error('aborted')), 40_000)),
      } as unknown as Response),
    )
    const p = fetchProject('flask')
    await vi.advanceTimersByTimeAsync(41_000) // 30s abort fires first, then the json read rejects
    expect(await p).toMatchObject({ kind: 'TRANSIENT' })
  })
})
