export const USER_AGENT = 'LFX-Insights-DocsReadiness/1.0 (+https://insights.linuxfoundation.org)'

export interface IProbeResult {
  ok: boolean
  status: number
  finalUrl: string
  contentType: string
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

export function domainOf(url: string): string {
  return new URL(url).hostname
}

export function normalizedDomain(url: string): string {
  return domainOf(url).replace(/^www\./, '')
}

export async function probe(url: string, timeoutMs = 10_000): Promise<IProbeResult> {
  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal: AbortSignal.timeout(timeoutMs),
      headers: { 'User-Agent': USER_AGENT },
    })

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

export async function fetchText(url: string, timeoutMs = 5_000): Promise<string | null> {
  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal: AbortSignal.timeout(timeoutMs),
      headers: { 'User-Agent': USER_AGENT },
    })

    if (!response.ok) {
      return null
    }

    return await response.text()
  } catch {
    return null
  }
}
