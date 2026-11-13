import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { fetchBulkDailyRange, fetchLastPublishedDay } from '../fetchDownloads'
import { FetchErrorKind } from '../types'

const fetchMock = vi.fn()

function respond(status: number, body: unknown) {
  fetchMock.mockResolvedValue(
    new Response(typeof body === 'string' ? body : JSON.stringify(body), { status }),
  )
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('fetchLastPublishedDay', () => {
  it('returns the end day of the all-packages last-day query', async () => {
    respond(200, { downloads: 4_100_000_000, start: '2026-11-11', end: '2026-11-11' })

    const result = await fetchLastPublishedDay()

    expect(result).toEqual({ day: '2026-11-11' })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(fetchMock.mock.calls[0][0]).toBe('https://api.npmjs.org/downloads/point/last-day')
  })

  it('reports a 429 as a rate limit', async () => {
    respond(429, 'error code: 1015')

    const result = await fetchLastPublishedDay()

    expect(result).toMatchObject({ kind: FetchErrorKind.RATE_LIMIT, statusCode: 429 })
  })

  it('reports a 5xx as transient', async () => {
    respond(503, 'unavailable')

    const result = await fetchLastPublishedDay()

    expect(result).toMatchObject({ kind: FetchErrorKind.TRANSIENT, statusCode: 503 })
  })

  it('reports a body that is not JSON as malformed', async () => {
    respond(200, 'not json')

    const result = await fetchLastPublishedDay()

    expect(result).toMatchObject({ kind: FetchErrorKind.MALFORMED, message: 'invalid JSON' })
  })

  it('reports a body without an end day as malformed', async () => {
    respond(200, { downloads: 1 })

    const result = await fetchLastPublishedDay()

    expect(result).toMatchObject({ kind: FetchErrorKind.MALFORMED })
  })
})

describe('fetchBulkDailyRange', () => {
  it('requests the range for the comma-joined names', async () => {
    respond(200, {})

    await fetchBulkDailyRange(['react', 'lodash'], '2026-11-01', '2026-11-11')

    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://api.npmjs.org/downloads/range/2026-11-01:2026-11-11/react,lodash',
    )
  })

  it('maps each package to its daily rows and leaves out null entries', async () => {
    respond(200, {
      react: {
        start: '2026-11-10',
        end: '2026-11-11',
        package: 'react',
        downloads: [
          { day: '2026-11-10', downloads: 10 },
          { day: '2026-11-11', downloads: 12 },
        ],
      },
      'no-such-package': null,
    })

    const result = await fetchBulkDailyRange(
      ['react', 'no-such-package'],
      '2026-11-10',
      '2026-11-11',
    )

    expect(result).toEqual({
      downloads: new Map([
        [
          'react',
          [
            { day: '2026-11-10', downloads: 10 },
            { day: '2026-11-11', downloads: 12 },
          ],
        ],
      ]),
    })
  })

  it('reports a 429 as a rate limit', async () => {
    respond(429, 'error code: 1015')

    const result = await fetchBulkDailyRange(['react', 'lodash'], '2026-11-10', '2026-11-11')

    expect(result).toMatchObject({ kind: FetchErrorKind.RATE_LIMIT, statusCode: 429 })
  })

  it('reports an entry without a downloads array as malformed instead of treating it as missing', async () => {
    respond(200, {
      react: { start: '2026-11-10', end: '2026-11-11', package: 'react', downloads: 123 },
      lodash: {
        start: '2026-11-10',
        end: '2026-11-11',
        package: 'lodash',
        downloads: [{ day: '2026-11-10', downloads: 3 }],
      },
    })

    const result = await fetchBulkDailyRange(['react', 'lodash'], '2026-11-10', '2026-11-11')

    expect(result).toMatchObject({ kind: FetchErrorKind.MALFORMED })
  })

  it('reports a single-package-shaped body as malformed', async () => {
    respond(200, {
      start: '2026-11-10',
      end: '2026-11-11',
      package: 'react',
      downloads: [{ day: '2026-11-10', downloads: 10 }],
    })

    const result = await fetchBulkDailyRange(['react', 'lodash'], '2026-11-10', '2026-11-11')

    expect(result).toMatchObject({ kind: FetchErrorKind.MALFORMED })
  })

  it('reports a non-object body as malformed', async () => {
    respond(200, '[1, 2]')

    const result = await fetchBulkDailyRange(['react', 'lodash'], '2026-11-10', '2026-11-11')

    expect(result).toMatchObject({ kind: FetchErrorKind.MALFORMED })
  })

  it('rejects one name, which npm answers in the single-package shape, and more than 128', async () => {
    const tooMany = Array.from({ length: 129 }, (_, i) => `p${i}`)

    await expect(fetchBulkDailyRange(['react'], '2026-11-10', '2026-11-11')).rejects.toThrow()
    await expect(fetchBulkDailyRange(tooMany, '2026-11-10', '2026-11-11')).rejects.toThrow()
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('request timeout', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('gives up on a body that stalls past the 30 second request timeout', async () => {
    vi.useFakeTimers()
    fetchMock.mockImplementation(
      async (_url: string, init: RequestInit) =>
        new Response(
          new ReadableStream({
            start(controller) {
              init.signal?.addEventListener('abort', () => controller.error(new Error('aborted')))
            },
          }),
          { status: 200 },
        ),
    )

    const pending = fetchLastPublishedDay()
    await vi.advanceTimersByTimeAsync(31_000)

    await expect(pending).resolves.toMatchObject({ kind: FetchErrorKind.TRANSIENT })
  }, 2_000)
})
