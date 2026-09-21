export const USER_AGENT = 'LFX-Insights-DocsReadiness/1.0 (+https://insights.linuxfoundation.org)'

export interface IProbeResult {
  ok: boolean
  status: number
  finalUrl: string
  contentType: string
}

const MAX_REDIRECTS = 5
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308])
const PRIVATE_HOSTNAME_RE = /^(localhost\.?|.+\.(local|internal|localdomain|localhost))$/i
const IPV4_RE = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/

function isPrivateIpv4(host: string): boolean {
  const octets = host.split('.').map(Number)
  if (octets.some((o) => Number.isNaN(o) || o > 255)) {
    return true
  }
  const [a, b] = octets
  return (
    a === 0 ||
    a === 10 ||
    a === 127 ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168)
  )
}

function isPrivateIpv6(addr: string): boolean {
  const a = addr.toLowerCase().replace(/^\[/, '').replace(/\]$/, '')
  if (a === '::1' || a === '::') {
    return true
  }
  const dottedMapped = a.match(/^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/)
  if (dottedMapped) {
    return isPrivateIpv4(dottedMapped[1])
  }
  // WHATWG URL parsing canonicalizes an IPv4-mapped address to hex groups, e.g.
  // ::ffff:127.0.0.1 -> ::ffff:7f00:1, so the dotted form above never matches a real hostname.
  const hexMapped = a.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/)
  if (hexMapped) {
    const hi = parseInt(hexMapped[1], 16)
    const lo = parseInt(hexMapped[2], 16)
    return isPrivateIpv4([hi >> 8, hi & 0xff, lo >> 8, lo & 0xff].join('.'))
  }
  if (/^f[cd][0-9a-f]{2}:/.test(a)) {
    return true // fc00::/7 unique-local
  }
  if (/^fe[89ab][0-9a-f]:/.test(a)) {
    return true // fe80::/10 link-local
  }
  return false
}

export function isPrivateOrLoopbackHost(hostname: string): boolean {
  const host = hostname.toLowerCase()
  if (PRIVATE_HOSTNAME_RE.test(host)) {
    return true
  }
  if (IPV4_RE.test(host)) {
    return isPrivateIpv4(host)
  }
  if (host.includes(':')) {
    return isPrivateIpv6(host)
  }
  return false
}

function isUnsafeUrl(raw: string): boolean {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return true
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    return true
  }
  return isPrivateOrLoopbackHost(url.hostname)
}

// Only rejects literal private/loopback hosts; a public hostname whose DNS record
// points at an internal address (DNS rebinding) is a known follow-up, not covered here.
async function guardedFetch(url: string, timeoutMs: number): Promise<Response | null> {
  const signal = AbortSignal.timeout(timeoutMs)
  let current = url
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    if (isUnsafeUrl(current)) {
      return null
    }
    const response = await fetch(current, {
      redirect: 'manual',
      signal,
      headers: { 'User-Agent': USER_AGENT },
    })
    if (REDIRECT_STATUSES.has(response.status)) {
      const location = response.headers.get('location')
      if (!location) {
        await response.body?.cancel()
        return null
      }
      current = new URL(location, current).toString()
      await response.body?.cancel()
      continue
    }
    return response
  }
  return null
}

export function normalizeUrl(raw: string): string | null {
  const trimmed = raw.trim()
  const withScheme = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(trimmed) ? trimmed : `https://${trimmed}`

  let parsed: URL
  try {
    parsed = new URL(withScheme)
  } catch {
    return null
  }

  parsed.hostname = parsed.hostname.toLowerCase()
  parsed.hash = ''

  let result = parsed.toString()
  if (parsed.pathname !== '/' && result.endsWith('/')) {
    result = result.slice(0, -1)
  }

  return result
}

export function domainOf(url: string): string | null {
  try {
    return new URL(url).hostname
  } catch {
    return null
  }
}

export function normalizedDomain(url: string): string | null {
  const normalized = normalizeUrl(url)
  return normalized ? (domainOf(normalized)?.replace(/^www\./, '') ?? null) : null
}

export async function probe(url: string, timeoutMs = 10_000): Promise<IProbeResult> {
  try {
    const response = await guardedFetch(url, timeoutMs)
    if (!response) {
      return { ok: false, status: 0, finalUrl: '', contentType: '' }
    }

    return {
      ok: response.ok,
      status: response.status,
      finalUrl: response.url,
      contentType: response.headers.get('content-type') ?? '',
    }
  } catch {
    return { ok: false, status: 0, finalUrl: '', contentType: '' }
  }
}

export async function isLiveDocs(url: string): Promise<boolean> {
  const result = await probe(url)
  return result.ok && result.contentType.includes('text/html')
}

const MAX_FETCH_TEXT_BYTES = 2 * 1024 * 1024

export async function fetchText(url: string, timeoutMs = 5_000): Promise<string | null> {
  try {
    const response = await guardedFetch(url, timeoutMs)
    if (!response || !response.ok || !response.body) {
      return null
    }

    const reader = response.body.getReader()
    const chunks: Uint8Array[] = []
    let total = 0
    for (;;) {
      const { done, value } = await reader.read()
      if (done) {
        break
      }
      total += value.length
      if (total > MAX_FETCH_TEXT_BYTES) {
        await reader.cancel()
        return null
      }
      chunks.push(value)
    }
    return Buffer.concat(chunks).toString('utf-8')
  } catch {
    return null
  }
}
