import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { crateSourceUrl, fetchCrateLatestVersion, fetchCrateVersions } from '../registryClient'

function fakeResponse(
  status: number,
  body?: Record<string, unknown>,
  headers: Record<string, string> = {},
): Response {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
    json: async () => body ?? {},
  } as unknown as Response
}

beforeEach(() => {
  process.env.SECURITY_CONTACTS_USER_AGENT = 'test-agent'
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
  delete process.env.SECURITY_CONTACTS_USER_AGENT
})

describe('fetchCrateVersions', () => {
  it('parses versions array from API response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        fakeResponse(200, {
          versions: [{ num: '1.0.0' }, { num: '1.1.0' }, { num: '2.0.0' }],
        }),
      ),
    )
    const result = await fetchCrateVersions('serde', 5000)
    expect(result).toEqual({ name: 'serde', versions: ['1.0.0', '1.1.0', '2.0.0'] })
  })

  it('filters out entries with missing num field', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        fakeResponse(200, {
          versions: [{ num: '1.0.0' }, { other: 'field' }, { num: '1.1.0' }],
        }),
      ),
    )
    const result = await fetchCrateVersions('serde', 5000)
    expect(result).toEqual({ name: 'serde', versions: ['1.0.0', '1.1.0'] })
  })

  it('resolves the canonical crate name from a versions entry', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        fakeResponse(200, {
          versions: [{ num: '1.0.0', crate: 'serde-json' }],
        }),
      ),
    )
    const result = await fetchCrateVersions('serde_json', 5000)
    expect(result).toEqual({ name: 'serde-json', versions: ['1.0.0'] })
  })

  it('maps a 404 to NOT_FOUND', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(404)))
    const result = await fetchCrateVersions('nonexistent-crate', 5000)
    expect(result).toMatchObject({ kind: 'NOT_FOUND', statusCode: 404 })
  })

  it('maps a 403 to NOT_FOUND', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(403)))
    const result = await fetchCrateVersions('forbidden-crate', 5000)
    expect(result).toMatchObject({ kind: 'NOT_FOUND', statusCode: 403 })
  })

  it('maps a 500 to TRANSIENT', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(500)))
    const result = await fetchCrateVersions('serde', 5000)
    expect(result).toMatchObject({ kind: 'TRANSIENT', statusCode: 500 })
  })

  it('maps malformed JSON to MALFORMED', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => {
        throw new Error('Invalid JSON')
      },
    } as unknown as Response)
    vi.stubGlobal('fetch', mockFetch)
    const result = await fetchCrateVersions('serde', 5000)
    expect(result).toMatchObject({ kind: 'MALFORMED', message: 'invalid json' })
  })

  it('maps missing versions array to MALFORMED', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, { crate: {} })))
    const result = await fetchCrateVersions('serde', 5000)
    expect(result).toMatchObject({ kind: 'MALFORMED', message: 'missing versions array' })
  })

  it('retries on 429 with Retry-After and eventually succeeds', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(fakeResponse(429, undefined, { 'retry-after': '0' }))
      .mockResolvedValueOnce(
        fakeResponse(200, {
          versions: [{ num: '1.0.0' }],
        }),
      )
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers()

    const promise = fetchCrateVersions('serde', 5000)
    await vi.runAllTimersAsync()

    const result = await promise
    expect(result).toEqual({ name: 'serde', versions: ['1.0.0'] })
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('gives up after max retries on 429', async () => {
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse(429))
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers()

    const promise = fetchCrateVersions('serde', 5000)
    await vi.runAllTimersAsync()

    const result = await promise
    expect(result).toMatchObject({ kind: 'RATE_LIMIT', statusCode: 429 })
    expect(fetchMock).toHaveBeenCalledTimes(6) // 0 to 5 inclusive
  })

  it('maps network error to TRANSIENT', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('ECONNRESET')))
    const result = await fetchCrateVersions('serde', 5000)
    expect(result).toMatchObject({ kind: 'TRANSIENT' })
  })
})

describe('fetchCrateLatestVersion', () => {
  it('returns newest_version when present', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        fakeResponse(200, {
          crate: { newest_version: '1.5.0', max_version: '1.4.0' },
        }),
      ),
    )
    const result = await fetchCrateLatestVersion('serde', 5000)
    expect(result).toEqual({ name: 'serde', version: '1.5.0' })
  })

  it('falls back to max_version when newest_version is missing', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        fakeResponse(200, {
          crate: { max_version: '1.4.0' },
        }),
      ),
    )
    const result = await fetchCrateLatestVersion('serde', 5000)
    expect(result).toEqual({ name: 'serde', version: '1.4.0' })
  })

  it('resolves the canonical crate name from crate.name', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        fakeResponse(200, {
          crate: { name: 'serde-json', newest_version: '1.0.0' },
        }),
      ),
    )
    const result = await fetchCrateLatestVersion('serde_json', 5000)
    expect(result).toEqual({ name: 'serde-json', version: '1.0.0' })
  })

  it('returns MALFORMED when neither newest_version nor max_version present', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(200, { crate: {} })))
    const result = await fetchCrateLatestVersion('serde', 5000)
    expect(result).toMatchObject({ kind: 'MALFORMED' })
  })

  it('maps a 404 to NOT_FOUND', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(fakeResponse(404)))
    const result = await fetchCrateLatestVersion('nonexistent-crate', 5000)
    expect(result).toMatchObject({ kind: 'NOT_FOUND', statusCode: 404 })
  })

  it('retries on 429 and eventually succeeds', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(fakeResponse(429, undefined, { 'retry-after': '0' }))
      .mockResolvedValueOnce(
        fakeResponse(200, {
          crate: { newest_version: '1.5.0' },
        }),
      )
    vi.stubGlobal('fetch', fetchMock)
    vi.useFakeTimers()

    const promise = fetchCrateLatestVersion('serde', 5000)
    await vi.runAllTimersAsync()

    const result = await promise
    expect(result).toEqual({ name: 'serde', version: '1.5.0' })
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })
})

describe('crateSourceUrl', () => {
  it('constructs the static.crates.io download URL with proper encoding', () => {
    const url = crateSourceUrl('serde', '1.0.0')
    expect(url).toBe('https://static.crates.io/crates/serde/serde-1.0.0.crate')
  })

  it('encodes special characters in crate names and versions', () => {
    const url = crateSourceUrl('my-crate', '1.0.0-rc.1+build')
    expect(url).toBe('https://static.crates.io/crates/my-crate/my-crate-1.0.0-rc.1%2Bbuild.crate')
  })
})
