import { afterEach, describe, expect, it, vi } from 'vitest'

import { fetchText, isLiveDocs, normalizeUrl, probe } from './http'

function jsonRouter(routes: Record<string, () => Response>) {
  return vi.fn((input: string | URL | Request) => {
    const url = typeof input === 'string' ? input : input.toString()
    const match = Object.keys(routes).find((prefix) => url.startsWith(prefix))
    if (!match) {
      return Promise.reject(new Error(`no route for ${url}`))
    }
    return Promise.resolve(routes[match]())
  })
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('normalizeUrl', () => {
  it('prefixes https:// when no scheme is present', () => {
    expect(normalizeUrl('example.com')).toBe('https://example.com/')
  })

  it('drops a trailing slash on a non-root path', () => {
    expect(normalizeUrl('https://example.com/docs/')).toBe('https://example.com/docs')
  })

  it('keeps the trailing slash for the root path', () => {
    expect(normalizeUrl('https://example.com/')).toBe('https://example.com/')
  })

  it('drops the hash fragment', () => {
    expect(normalizeUrl('https://example.com/docs#section')).toBe('https://example.com/docs')
  })

  it('lowercases the host', () => {
    expect(normalizeUrl('https://Example.COM')).toBe('https://example.com/')
  })

  it('returns null for invalid input', () => {
    expect(normalizeUrl('::not a url::')).toBeNull()
  })
})

describe('probe', () => {
  it('reports ok/status/finalUrl/contentType on an html response', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () =>
          new Response('<html></html>', {
            status: 200,
            headers: { 'content-type': 'text/html; charset=utf-8' },
          }),
      }),
    )

    const result = await probe('https://example.com/')
    expect(result).toEqual({
      ok: true,
      status: 200,
      finalUrl: '',
      contentType: 'text/html; charset=utf-8',
    })
  })

  it('never throws on a fetch rejection', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('network error'))),
    )

    await expect(probe('https://example.com')).resolves.toEqual({
      ok: false,
      status: 0,
      finalUrl: '',
      contentType: '',
    })
  })
})

describe('isLiveDocs', () => {
  it('is true only for an ok html response', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () =>
          new Response('<html></html>', { status: 200, headers: { 'content-type': 'text/html' } }),
      }),
    )

    expect(await isLiveDocs('https://example.com')).toBe(true)
  })

  it('is false for a non-html content type', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () =>
          new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
      }),
    )

    expect(await isLiveDocs('https://example.com')).toBe(false)
  })

  it('is false for a non-ok status', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () =>
          new Response('not found', { status: 404, headers: { 'content-type': 'text/html' } }),
      }),
    )

    expect(await isLiveDocs('https://example.com')).toBe(false)
  })

  it('is false when fetch throws', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('timeout'))),
    )

    expect(await isLiveDocs('https://example.com')).toBe(false)
  })
})

describe('fetchText', () => {
  it('returns the body text on an ok response', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () => new Response('hello world', { status: 200 }),
      }),
    )

    expect(await fetchText('https://example.com')).toBe('hello world')
  })

  it('returns null on a non-ok status', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () => new Response('nope', { status: 500 }),
      }),
    )

    expect(await fetchText('https://example.com')).toBeNull()
  })

  it('returns null when fetch throws', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('timeout'))),
    )

    expect(await fetchText('https://example.com')).toBeNull()
  })
})
