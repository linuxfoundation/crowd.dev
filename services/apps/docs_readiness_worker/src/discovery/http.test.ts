import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  domainOf,
  fetchText,
  isLiveDocs,
  isPrivateOrLoopbackHost,
  normalizeUrl,
  normalizedDomain,
  probe,
} from './http'

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

describe('domainOf', () => {
  it('returns the hostname for a valid url', () => {
    expect(domainOf('https://docs.example.com/path')).toBe('docs.example.com')
  })

  it('returns null for malformed input', () => {
    expect(domainOf('::not a url::')).toBeNull()
  })
})

describe('normalizedDomain', () => {
  it('strips a leading www.', () => {
    expect(normalizedDomain('https://www.example.com')).toBe('example.com')
  })

  it('returns null for malformed input', () => {
    expect(normalizedDomain('::not a url::')).toBeNull()
  })
})

describe('isPrivateOrLoopbackHost', () => {
  it.each([
    ['127.0.0.1', 'ipv4 loopback'],
    ['::1', 'ipv6 loopback'],
    ['169.254.169.254', 'ipv4 link-local / cloud metadata'],
    ['fe80::1', 'ipv6 link-local'],
    ['10.0.0.5', 'ipv4 rfc1918 10/8'],
    ['172.16.0.5', 'ipv4 rfc1918 172.16/12'],
    ['192.168.1.5', 'ipv4 rfc1918 192.168/16'],
    ['localhost', 'localhost hostname'],
    ['myservice.internal', 'internal-suffixed hostname'],
    ['myservice.local', 'local-suffixed hostname'],
  ])('is true for %s (%s)', (host) => {
    expect(isPrivateOrLoopbackHost(host)).toBe(true)
  })

  it('is false for a public hostname', () => {
    expect(isPrivateOrLoopbackHost('example.com')).toBe(false)
  })

  it('is false for a public ipv4 address', () => {
    expect(isPrivateOrLoopbackHost('93.184.216.34')).toBe(false)
  })
})

describe('probe SSRF guard', () => {
  it('never calls fetch for a loopback address', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://127.0.0.1/admin')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('never calls fetch for a cloud metadata address', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://169.254.169.254/latest/meta-data/')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('never calls fetch for a private rfc1918 address', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://10.0.0.5/internal')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('never calls fetch for a non-http(s) scheme', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('file:///etc/passwd')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('stops following a redirect chain that points at a private address', async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        new Response(null, { status: 302, headers: { location: 'http://169.254.169.254/' } }),
      ),
    )
    vi.stubGlobal('fetch', fetchMock)

    const result = await probe('https://example.com/redirect')

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(result.ok).toBe(false)
  })

  it('still fetches a genuinely public url', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () =>
          new Response('<html></html>', { status: 200, headers: { 'content-type': 'text/html' } }),
      }),
    )

    const result = await probe('https://example.com/')
    expect(result.ok).toBe(true)
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

  it('never calls fetch for a loopback address', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    expect(await fetchText('http://127.0.0.1/admin')).toBeNull()
    expect(fetchMock).not.toHaveBeenCalled()
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
