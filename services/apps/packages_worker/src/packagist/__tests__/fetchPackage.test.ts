import { afterEach, describe, expect, it, vi } from 'vitest'

import { buildPackagistUserAgent, fetchPackagistP2, fetchPackagistStats } from '../fetchPackage'

// Minimal Response stand-in for the global fetch mock. `body.cancel` is a spy so
// error-path tests can assert the response body is drained before returning.
function fakeResponse(
  status: number,
  body?: unknown,
  opts: { jsonThrows?: boolean; lastModified?: string } = {},
): Response & { body: { cancel: ReturnType<typeof vi.fn> } } {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: {
      get: (k: string) =>
        k.toLowerCase() === 'last-modified' ? (opts.lastModified ?? null) : null,
    },
    json: async () => {
      if (opts.jsonThrows) throw new Error('bad json')
      return body
    },
    body: { cancel: vi.fn() },
  } as unknown as Response & { body: { cancel: ReturnType<typeof vi.fn> } }
}

const validStats = { package: { name: 'monolog/monolog', downloads: { monthly: 5 } } }
const validP2 = {
  packages: { 'monolog/monolog': [{ version: '2.0.0' }, { version: '1.0.0' }] },
  minified: 'composer/2.0',
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
  delete process.env.CROWD_PACKAGES_PACKAGIST_MAILTO
})

// B1 — polite crawling: UA carries a mailto, configurable via env.
describe('buildPackagistUserAgent', () => {
  it('includes a mailto contact by default', () => {
    expect(buildPackagistUserAgent()).toMatch(/mailto=/)
  })

  it('uses the env-configured mailto when set', () => {
    process.env.CROWD_PACKAGES_PACKAGIST_MAILTO = 'oss@example.org'
    expect(buildPackagistUserAgent()).toContain('mailto=oss@example.org')
  })
})

// B1 — dynamic (stats) endpoint: status → FetchError kind mapping
describe('fetchPackagistStats', () => {
  it('returns the parsed body on 200 with a valid shape, from the dynamic endpoint URL', async () => {
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse(200, validStats))
    vi.stubGlobal('fetch', fetchMock)

    expect(await fetchPackagistStats('monolog/monolog')).toEqual(validStats)
    expect(fetchMock.mock.calls[0][0]).toBe('https://packagist.org/packages/monolog/monolog.json')
  })

  it('sends a User-Agent containing a mailto', async () => {
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse(200, validStats))
    vi.stubGlobal('fetch', fetchMock)

    await fetchPackagistStats('monolog/monolog')
    const headers = fetchMock.mock.calls[0][1].headers as Record<string, string>
    expect(headers['User-Agent']).toMatch(/mailto=/)
  })

  it('maps 404 → NOT_FOUND and drains the response body', async () => {
    const res = fakeResponse(404)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(res))
    expect(await fetchPackagistStats('gone/gone')).toMatchObject({
      kind: 'NOT_FOUND',
      statusCode: 404,
    })
    // an undrained body can pin the socket instead of returning it to the shared pool
    expect(res.body.cancel).toHaveBeenCalled()
  })

  it('maps 429 → RATE_LIMIT and drains the response body', async () => {
    const res = fakeResponse(429)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(res))
    expect(await fetchPackagistStats('busy/busy')).toMatchObject({
      kind: 'RATE_LIMIT',
      statusCode: 429,
    })
    expect(res.body.cancel).toHaveBeenCalled()
  })

  it('maps other non-ok statuses (5xx) → TRANSIENT with the status code and drains the body', async () => {
    const res = fakeResponse(503)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(res))
    expect(await fetchPackagistStats('flaky/flaky')).toMatchObject({
      kind: 'TRANSIENT',
      statusCode: 503,
    })
    expect(res.body.cancel).toHaveBeenCalled()
  })

  it('maps a network rejection → TRANSIENT without a status code', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('ECONNRESET')))
    const result = await fetchPackagistStats('monolog/monolog')
    expect(result).toMatchObject({ kind: 'TRANSIENT' })
    expect((result as { statusCode?: number }).statusCode).toBeUndefined()
  })

  it('maps an unparseable body → MALFORMED', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(fakeResponse(200, undefined, { jsonThrows: true })),
    )
    expect(await fetchPackagistStats('monolog/monolog')).toMatchObject({ kind: 'MALFORMED' })
  })

  it('maps an unexpected JSON shape → MALFORMED', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, { not: 'a package' })))
    expect(await fetchPackagistStats('monolog/monolog')).toMatchObject({ kind: 'MALFORMED' })
  })

  // Regression: a technically-valid-JSON body with the wrong runtime type for a field
  // normalizePackagistStats consumes unconditionally must be classified MALFORMED, not
  // thrown past the guard (where it would be misread as a transient failure and
  // retried forever on the same deterministic input).
  it.each([
    ['description is a number', { package: { name: 'a/b', description: 123 } }],
    ['repository is an object', { package: { name: 'a/b', repository: {} } }],
    ['maintainers is not an array', { package: { name: 'a/b', maintainers: {} } }],
  ])('maps a wrong-typed field (%s) → MALFORMED, not a throw', async (_desc, body) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, body)))
    await expect(fetchPackagistStats('monolog/monolog')).resolves.toMatchObject({
      kind: 'MALFORMED',
    })
  })

  it('maps a body read aborted by the 30s timeout → TRANSIENT (not MALFORMED)', async () => {
    vi.useFakeTimers()
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        status: 200,
        ok: true,
        headers: { get: () => null },
        json: () =>
          new Promise((_resolve, reject) => setTimeout(() => reject(new Error('aborted')), 40_000)),
      } as unknown as Response),
    )
    const p = fetchPackagistStats('monolog/monolog')
    await vi.advanceTimersByTimeAsync(41_000)
    expect(await p).toMatchObject({ kind: 'TRANSIENT' })
  })
})

// B1 + C5 — static p2 endpoint: same error contract plus If-Modified-Since / 304 handling.
describe('fetchPackagistP2', () => {
  it('returns the minified versions and Last-Modified on 200, from the static endpoint URL', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        fakeResponse(200, validP2, { lastModified: 'Wed, 01 Jul 2026 00:00:00 GMT' }),
      )
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchPackagistP2('monolog/monolog', null)
    expect(result).toEqual({
      minifiedVersions: [{ version: '2.0.0' }, { version: '1.0.0' }],
      lastModified: 'Wed, 01 Jul 2026 00:00:00 GMT',
    })
    expect(fetchMock.mock.calls[0][0]).toBe('https://repo.packagist.org/p2/monolog/monolog.json')
  })

  it('sends If-Modified-Since when a previous Last-Modified is known', async () => {
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse(200, validP2))
    vi.stubGlobal('fetch', fetchMock)

    await fetchPackagistP2('monolog/monolog', 'Tue, 30 Jun 2026 00:00:00 GMT')
    const headers = fetchMock.mock.calls[0][1].headers as Record<string, string>
    expect(headers['If-Modified-Since']).toBe('Tue, 30 Jun 2026 00:00:00 GMT')
  })

  it('omits If-Modified-Since when none is known', async () => {
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse(200, validP2))
    vi.stubGlobal('fetch', fetchMock)

    await fetchPackagistP2('monolog/monolog', null)
    const headers = fetchMock.mock.calls[0][1].headers as Record<string, string>
    expect('If-Modified-Since' in headers).toBe(false)
  })

  it('maps 304 → NOT_MODIFIED', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(304)))
    expect(await fetchPackagistP2('monolog/monolog', 'Tue, 30 Jun 2026 00:00:00 GMT')).toEqual({
      kind: 'NOT_MODIFIED',
    })
  })

  it('maps 404 → NOT_FOUND and 429 → RATE_LIMIT, draining the body each time', async () => {
    const notFound = fakeResponse(404)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(notFound))
    expect(await fetchPackagistP2('gone/gone', null)).toMatchObject({ kind: 'NOT_FOUND' })
    expect(notFound.body.cancel).toHaveBeenCalled()

    const rateLimited = fakeResponse(429)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(rateLimited))
    expect(await fetchPackagistP2('busy/busy', null)).toMatchObject({ kind: 'RATE_LIMIT' })
    expect(rateLimited.body.cancel).toHaveBeenCalled()
  })

  it('maps a payload missing the package key → MALFORMED', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(fakeResponse(200, { packages: { 'other/pkg': [] } })),
    )
    expect(await fetchPackagistP2('monolog/monolog', null)).toMatchObject({ kind: 'MALFORMED' })
  })

  // Regression: a malformed element in the version array must be classified
  // MALFORMED, not thrown past the guard — expandComposerMetadata's Object.entries()
  // throws on a null/non-object element, and version/version_normalized/license reach
  // .startsWith()/.split()/.endsWith() and the SQL text[] write unconditionally.
  it.each([
    ['an element is null', [null]],
    ['version is missing', [{ time: '2024-01-01' }]],
    ['version is a number', [{ version: 123 }]],
    ['version_normalized is a number', [{ version: '1.0.0', version_normalized: 42 }]],
    ['license is a scalar string, not an array', [{ version: '1.0.0', license: 'MIT' }]],
    ['homepage is an object', [{ version: '1.0.0', homepage: {} }]],
    ['time is a number', [{ version: '1.0.0', time: 20240101 }]],
  ])('maps a malformed version entry (%s) → MALFORMED, not a throw', async (_desc, versions) => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(fakeResponse(200, { packages: { 'monolog/monolog': versions } })),
    )
    await expect(fetchPackagistP2('monolog/monolog', null)).resolves.toMatchObject({
      kind: 'MALFORMED',
    })
  })

  it('accepts a version entry using the __unset diff sentinel on optional fields', async () => {
    const versions = [
      { version: '1.0.0', license: ['MIT'] },
      { version: '2.0.0', license: '__unset' },
    ]
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(fakeResponse(200, { packages: { 'monolog/monolog': versions } })),
    )
    const result = await fetchPackagistP2('monolog/monolog', null)
    expect(result).not.toMatchObject({ kind: 'MALFORMED' })
  })
})
