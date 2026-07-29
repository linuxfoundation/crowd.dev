import { createHash } from 'node:crypto'
import { lookup } from 'node:dns/promises'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import type { githubApiGet } from '../githubToken'

const MAX_PAGE_BYTES = 500_000
const MAX_REDIRECTS = 3
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308])

const BLOCKED_HOST_RE = /^(localhost|metadata\.google\.internal|.+\.(local|internal|localdomain))$/i
const IPV4_RE = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/

export function sha256Hex(text: string): string {
  return createHash('sha256').update(text).digest('hex')
}

export async function fetchBlob(
  deps: { githubGet: typeof githubApiGet },
  repoUrl: string,
  blobOid: string,
  timeoutMs: number,
): Promise<string | null> {
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(repoUrl))
  } catch {
    return null
  }
  const { text } = await deps.githubGet(`/repos/${owner}/${name}/git/blobs/${blobOid}`, timeoutMs, {
    raw: true,
  })
  return text
}

export function htmlToText(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script\b[^>]*>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style\b[^>]*>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#?(?!amp;)\w+;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
}

function isPrivateIpv4(host: string): boolean {
  const octets = host.split('.').map(Number)
  if (octets.some((o) => o > 255)) return true
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

function isBlockedIpv6(addr: string): boolean {
  const a = addr.toLowerCase()
  if (a === '::1' || a === '::') return true
  const mapped = a.match(/^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/)
  if (mapped) return isPrivateIpv4(mapped[1])
  if (/^f[cd][0-9a-f]{2}:/.test(a)) return true // fc00::/7 unique-local
  if (/^fe[89ab][0-9a-f]:/.test(a)) return true // fe80::/10 link-local
  return false
}

export function isBlockedUrl(raw: string): boolean {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return true
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return true
  const host = url.hostname
  if (BLOCKED_HOST_RE.test(host)) return true
  if (host.includes(':') || host.startsWith('[')) return true
  if (IPV4_RE.test(host)) return isPrivateIpv4(host)
  return false
}

// The static isBlockedUrl check only rejects literal private IPs; a public
// hostname whose DNS record points at 127.0.0.1, RFC1918, or a metadata
// address slips through. Resolve every hop and validate the answers before
// connecting. This narrows but does not fully close the DNS-rebinding window
// (fetch resolves again internally); acceptable for a daily internal batch
// worker fetching public policy pages.
async function resolvesToBlockedAddress(host: string): Promise<boolean> {
  let addrs: Array<{ address: string; family: number }>
  try {
    addrs = await lookup(host, { all: true })
  } catch {
    return true
  }
  if (addrs.length === 0) return true
  return addrs.some(({ address, family }) =>
    family === 6 ? isBlockedIpv6(address) : isPrivateIpv4(address),
  )
}

async function isRequestBlocked(raw: string): Promise<boolean> {
  if (isBlockedUrl(raw)) return true
  const host = new URL(raw).hostname
  if (IPV4_RE.test(host)) return false // public IPv4 literal already validated
  return resolvesToBlockedAddress(host)
}

async function fetchGuarded(url: string, signal: AbortSignal): Promise<Response | null> {
  let current = url
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    if (await isRequestBlocked(current)) return null
    const res = await fetch(current, {
      signal,
      headers: { 'User-Agent': 'crowd.dev-reporting-protocol' },
      redirect: 'manual',
    })
    if (REDIRECT_STATUSES.has(res.status)) {
      const location = res.headers.get('location')
      if (!location) return null
      current = new URL(location, current).toString()
      continue
    }
    return res
  }
  return null
}

async function readBodyCapped(res: Response, maxBytes: number): Promise<string> {
  const reader = res.body?.getReader()
  if (!reader) {
    return (await res.text()).slice(0, maxBytes)
  }
  const chunks: Uint8Array[] = []
  let received = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    if (value) {
      chunks.push(value)
      received += value.byteLength
      if (received >= maxBytes) {
        await reader.cancel()
        break
      }
    }
  }
  const merged = new Uint8Array(Math.min(received, maxBytes))
  let offset = 0
  for (const chunk of chunks) {
    const take = Math.min(chunk.byteLength, merged.byteLength - offset)
    if (take <= 0) break
    merged.set(chunk.subarray(0, take), offset)
    offset += take
  }
  return new TextDecoder().decode(merged)
}

export async function fetchLinkedPage(
  url: string,
  timeoutMs: number,
): Promise<{ text: string; hash: string } | null> {
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetchGuarded(url, controller.signal)
    if (!res || !res.ok) return null
    const raw = await readBodyCapped(res, MAX_PAGE_BYTES)
    const text = htmlToText(raw)
    if (!text) return null
    return { text, hash: sha256Hex(text) }
  } catch {
    return null
  } finally {
    clearTimeout(timeoutHandle)
  }
}
