import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  domainOf,
  fetchText,
  isLiveDocs,
  isPrivateOrLoopbackHost,
  normalizeUrl,
  normalizedDomain,
  isTrustedRedirect,
  PROBE_RETRY_BACKOFF_MS,
  PROBE_TIMEOUT_MS,
  probe,
  probeRetry,
  sameRegistrableDomain,
} from './http'

function jsonRouter(routes: Record<string, () => Response>) {
  return vi.fn((input: string | URL | Request) => {
    const url = typeof input === 'string' ? input : input.toString()
    const match = Object.keys(routes).find((prefix) => url.startsWith(prefix))
    if (!match) {
      return Promise.reject(new Error(`no route for ${url}`))
    }
    const response = routes[match]()
    if (!response.url) {
      Object.defineProperty(response, 'url', { value: url })
    }
    return Promise.resolve(response)
  })
}

afterEach(() => {
  vi.unstubAllGlobals()
})

function htmlAt(finalUrl: string, body = '<html></html>') {
  const response = new Response(body, { status: 200, headers: { 'content-type': 'text/html' } })
  Object.defineProperty(response, 'url', { value: finalUrl })
  return response
}

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
      finalUrl: 'https://example.com/',
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

  it('cancels the response body after reading metadata', async () => {
    const response = new Response('<html></html>', {
      status: 200,
      headers: { 'content-type': 'text/html' },
    })
    const cancel = vi.spyOn(response.body as ReadableStream, 'cancel')
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(response)),
    )

    await probe('https://example.com/')

    expect(cancel).toHaveBeenCalledOnce()
  })
})

describe('probe retry', () => {
  const html = () => htmlAt('https://example.com/')
  const withRetry = (url: string) => probe(url, PROBE_TIMEOUT_MS, true)

  beforeEach(() => {
    probeRetry.backoffMs = 0
  })

  it('retries once after a timeout and returns the second result', async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new DOMException('timed out', 'TimeoutError'))
      .mockResolvedValueOnce(html())
    vi.stubGlobal('fetch', fetchMock)

    expect((await withRetry('https://example.com/')).ok).toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('retries once after a 5xx and gives up after the second failure', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response('', { status: 503 })))
    vi.stubGlobal('fetch', fetchMock)

    const result = await withRetry('https://example.com/')

    expect(result.ok).toBe(false)
    expect(result.status).toBe(503)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('does not retry a 404', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response('', { status: 404 })))
    vi.stubGlobal('fetch', fetchMock)

    expect((await withRetry('https://example.com/')).status).toBe(404)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('does not retry a blocked url', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await withRetry('http://127.0.0.1/')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('does not retry unless asked to', async () => {
    const fetchMock = vi.fn(() => Promise.reject(new Error('reset')))
    vi.stubGlobal('fetch', fetchMock)

    await probe('https://example.com/')
    await isLiveDocs('https://example.com/')

    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('isLiveDocs is true when the first attempt fails and the retry succeeds', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockRejectedValueOnce(new Error('reset')).mockResolvedValueOnce(html()),
    )

    expect(await isLiveDocs('https://example.com/', true)).toBe(true)
  })

  it('gives every attempt the same 10s timeout so a retried probe costs at most 20s', async () => {
    const timeout = vi.spyOn(AbortSignal, 'timeout')
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('reset')))

    await withRetry('https://example.com/')

    expect(PROBE_TIMEOUT_MS).toBe(10_000)
    expect(timeout.mock.calls.map(([ms]) => ms)).toEqual([PROBE_TIMEOUT_MS, PROBE_TIMEOUT_MS])
    timeout.mockRestore()
  })

  it('retries once after a 429 and returns the second result', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response('', { status: 429 }))
      .mockResolvedValueOnce(html())
    vi.stubGlobal('fetch', fetchMock)

    expect((await withRetry('https://example.com/')).ok).toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })
})

describe('probe retry timing', () => {
  const hangUntilAborted = (_url: string, init?: RequestInit) =>
    new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => reject(new Error('timed out')))
    })

  async function timed(fetchMock: ReturnType<typeof vi.fn>) {
    vi.useFakeTimers()
    probeRetry.backoffMs = PROBE_RETRY_BACKOFF_MS
    vi.spyOn(AbortSignal, 'timeout').mockImplementation((ms: number) => {
      const controller = new AbortController()
      setTimeout(() => controller.abort(new DOMException('timed out', 'TimeoutError')), ms)
      return controller.signal
    })
    vi.stubGlobal('fetch', fetchMock)
    const startedAt = Date.now()
    let elapsedMs = -1
    const done = probe('https://example.com/', PROBE_TIMEOUT_MS, true).then((result) => {
      elapsedMs = Date.now() - startedAt
      return result
    })
    await vi.advanceTimersByTimeAsync(60_000)
    return { result: await done, elapsedMs }
  }

  afterEach(() => {
    probeRetry.backoffMs = 0
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('answers live when a slow first attempt is followed by a live one after the backoff', async () => {
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(hangUntilAborted)
      .mockResolvedValueOnce(htmlAt('https://example.com/'))

    const { result } = await timed(fetchMock)

    expect(result.ok).toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('returns not live for a dead host after two attempts, within two timeouts plus the backoff', async () => {
    const fetchMock = vi.fn(hangUntilAborted)

    const { result, elapsedMs } = await timed(fetchMock)

    expect(result.ok).toBe(false)
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(elapsedMs).toBe(2 * PROBE_TIMEOUT_MS + PROBE_RETRY_BACKOFF_MS)
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

  it('is true for a differently-cased content type', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () =>
          new Response('<html></html>', {
            status: 200,
            headers: { 'content-type': 'Text/HTML; charset=UTF-8' },
          }),
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

  it('is true when the final url stays on the same site (http to https, www, subdomain, path)', async () => {
    for (const [url, finalUrl] of [
      ['http://example.com', 'https://example.com/'],
      ['https://example.com', 'https://www.example.com/'],
      ['https://example.org', 'https://docs.example.org/en/'],
      ['https://foo.github.io', 'https://foo.github.io/repo/'],
    ]) {
      vi.stubGlobal('fetch', jsonRouter({ [url]: () => htmlAt(finalUrl) }))
      expect(await isLiveDocs(url)).toBe(true)
    }
  })

  it('is false when the redirect lands on another registrable domain', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({ 'https://zotregistry.io': () => htmlAt('https://spam-casino.example.net/') }),
    )

    expect(await isLiveDocs('https://zotregistry.io')).toBe(false)
  })

  it('is false when one github.io site redirects to another', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({ 'https://foo.github.io': () => htmlAt('https://bar.github.io/') }),
    )

    expect(await isLiveDocs('https://foo.github.io')).toBe(false)
  })

  it('is false for an ip-literal url', async () => {
    vi.stubGlobal('fetch', jsonRouter({ 'http://8.8.8.8': () => htmlAt('http://8.8.8.8/') }))

    expect(await isLiveDocs('http://8.8.8.8')).toBe(false)
  })
})

describe('isTrustedRedirect', () => {
  it('allows the benign redirects seen in the POC fixture', () => {
    for (const [url, finalUrl] of [
      ['https://cloudnative-pg.github.io/docs', 'https://cloudnative-pg.io/docs/'],
      ['https://c2pa-org.github.io', 'https://spec.c2pa.org/'],
      ['https://ludwig-ai.github.io/ludwig-docs', 'https://ludwig.ai/'],
      ['https://envoy-mobile.github.io', 'https://envoymobile.io/'],
      ['https://docs.opentimeline.io', 'https://opentimelineio.readthedocs.io/en/stable/'],
      [
        'https://wiki.opendaylight.org/display/ODL/MD-SAL',
        'https://lf-opendaylight.atlassian.net/wiki/spaces/ODL',
      ],
      [
        'https://guacamole.readthedocs.org/en/latest/',
        'https://guacamole.readthedocs.io/en/latest/',
      ],
      ['https://wiki.o-ran-sc.org/x', 'https://lf-o-ran-sc.atlassian.net/wiki'],
      ['https://envoy-mobile.io', 'https://envoymobile.readthedocs.io/'],
      ['http://metallb.org', 'https://metallb.io/'],
      ['https://example.com', 'https://www.example.com/'],
      ['https://docs.opea.dev', 'https://opea-project.github.io/latest/index.html'],
    ]) {
      expect(isTrustedRedirect(url, finalUrl)).toBe(true)
    }
  })

  it('rejects redirects to an unrelated domain', () => {
    for (const [url, finalUrl] of [
      ['https://zotregistry.io', 'https://honda.org.mx/'],
      ['https://jenkins-x.io', 'https://jayex.io/'],
      ['https://foo.github.io', 'https://bar.github.io/'],
      ['https://foo.github.io', 'https://spam-casino.net/'],
      ['https://api.github.io', 'https://apiary-spam.com/'],
      ['https://zotregistry.io', 'https://casino-spam.atlassian.net/wiki/spaces/X'],
      ['https://zotregistry.io', 'https://casino-spam.readthedocs.io/en/latest/'],
      ['https://wiki.opendaylight.org', 'https://lf-anuket.atlassian.net/wiki'],
      ['https://ray.io', 'https://array-casino.atlassian.net/wiki'],
      ['https://p4.org', 'https://p4-casino.atlassian.net/wiki'],
      ['https://fd.io', 'https://xfdx.readthedocs.io/en/latest/'],
      ['https://docs.mycorp.io', 'https://docs.readthedocs.io/'],
      ['https://zotregistry.io', 'https://zotregistry-x.fakeatlassian.net/'],
      ['https://wiki.opendaylight.org', 'https://opendaylight-casino.atlassian.net/wiki'],
      ['https://opendaylight.org', 'https://opendaylight-casino.readthedocs.io/en/latest/'],
      ['https://zotregistry.io', 'https://zotregistry-docs.readthedocs.io/'],
      ['https://docs.opentimeline.io', 'https://opentimelineio-casino.readthedocs.io/'],
      ['https://foo-casino.github.io', 'https://foo.com'],
      ['https://ab-org.github.io', 'https://ab.com'],
      ['https://foo-org.io', 'https://foo.net'],
      ['http://8.8.8.8/', 'https://example.com/'],
      ['https://example.com', ''],
      ['https://docs.flyte.org', 'https://www.union.ai/'],
      ['https://docs.opea.dev', 'https://unrelated.github.io/latest/'],
      ['https://docs.opea.dev', 'https://opea-casino.github.io/'],
      ['https://example.com', 'https://evil.readthedocs.io.attacker.net/'],
    ]) {
      expect(isTrustedRedirect(url, finalUrl)).toBe(false)
    }
  })

  it('allows zotregistry.io moving to zotregistry.dev (same label)', () => {
    expect(isTrustedRedirect('https://zotregistry.io', 'https://zotregistry.dev/')).toBe(true)
  })
})

describe('sameRegistrableDomain', () => {
  it('treats a unicode host and its punycode form as the same site', () => {
    expect(sameRegistrableDomain('https://münchen.de', 'https://xn--mnchen-3ya.de/')).toBe(true)
    expect(sameRegistrableDomain('https://münchen.de', 'https://example.com/')).toBe(false)
  })

  it('compares registrable domains and fails closed on an empty final url', () => {
    expect(sameRegistrableDomain('https://a.example.com', 'https://b.example.com/x')).toBe(true)
    expect(sameRegistrableDomain('https://example.com', 'https://example.net')).toBe(false)
    expect(sameRegistrableDomain('https://example.com', '')).toBe(false)
    expect(sameRegistrableDomain('not a url', '')).toBe(false)
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

  it('resolves a scheme-less domain instead of returning null', () => {
    expect(normalizedDomain('example.com')).toBe('example.com')
  })
})

describe('isPrivateOrLoopbackHost', () => {
  it.each([
    ['127.0.0.1', 'ipv4 loopback'],
    ['::1', 'ipv6 loopback'],
    ['169.254.169.254', 'ipv4 link-local / cloud metadata'],
    ['fe80::1', 'ipv6 link-local'],
    ['fc00::1', 'ipv6 unique-local'],
    ['10.0.0.5', 'ipv4 rfc1918 10/8'],
    ['172.16.0.5', 'ipv4 rfc1918 172.16/12'],
    ['192.168.1.5', 'ipv4 rfc1918 192.168/16'],
    ['localhost', 'localhost hostname'],
    ['localhost.', 'trailing-dot localhost hostname'],
    ['foo.localhost', 'localhost subdomain'],
    ['myservice.internal', 'internal-suffixed hostname'],
    ['myservice.local', 'local-suffixed hostname'],
    ['myservice.internal.', 'trailing-dot internal-suffixed hostname'],
    ['metadata', 'unqualified single-label hostname'],
  ])('is true for %s (%s)', (host) => {
    expect(isPrivateOrLoopbackHost(host)).toBe(true)
  })

  it.each([
    ['::1', 'ipv6 loopback'],
    ['fe80::1', 'ipv6 link-local'],
    ['fc00::1', 'ipv6 unique-local'],
    ['::ffff:127.0.0.1', 'ipv4-mapped ipv6 loopback'],
  ])('is true for %s (%s) via URL.hostname bracketed form', (host) => {
    expect(isPrivateOrLoopbackHost(new URL(`http://[${host}]/`).hostname)).toBe(true)
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

  it('never calls fetch for an ipv6 loopback address', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://[::1]/')

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

  it('never calls fetch for an ipv4-mapped ipv6 loopback address', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://[::ffff:127.0.0.1]/')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('never calls fetch for a trailing-dot private-suffix hostname', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://service.internal./')

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('never calls fetch for an unqualified single-label hostname', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await probe('http://metadata/latest/meta-data/')

    expect(fetchMock).not.toHaveBeenCalled()
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

  it('returns null on a cross-domain redirect only when same-site is required', async () => {
    const stub = () =>
      vi.stubGlobal(
        'fetch',
        jsonRouter({
          'https://example.com': () => {
            const response = new Response('x'.repeat(60), { status: 200 })
            Object.defineProperty(response, 'url', { value: 'https://elsewhere.net/llms.txt' })
            return response
          },
        }),
      )

    stub()
    expect(await fetchText('https://example.com/llms.txt', 5_000, true)).toBeNull()
    stub()
    expect(await fetchText('https://example.com/llms.txt')).toBe('x'.repeat(60))
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

  it('returns null when the response body exceeds the size limit', async () => {
    vi.stubGlobal(
      'fetch',
      jsonRouter({
        'https://example.com': () => new Response('x'.repeat(2 * 1024 * 1024 + 1), { status: 200 }),
      }),
    )

    expect(await fetchText('https://example.com')).toBeNull()
  })
})
