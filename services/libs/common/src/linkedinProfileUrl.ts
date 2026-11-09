const LINKEDIN_HANDLE_RE = /^[^\s/\\?#]+$/

export function normalizeLinkedinHandle(raw: string | null | undefined): string | null {
  const handle = raw?.trim().replace(/^\/+|\/+$/g, '')
  return handle && LINKEDIN_HANDLE_RE.test(handle) ? handle : null
}

export function linkedinProfileHandle(raw: string | null | undefined): string | null {
  const trimmed = raw?.trim()
  if (!trimmed) {
    return null
  }

  let url: URL
  try {
    url = new URL(/^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`)
  } catch {
    return null
  }

  const host = url.hostname.toLowerCase()
  if (host !== 'linkedin.com' && !host.endsWith('.linkedin.com')) {
    return null
  }

  const [kind, segment] = url.pathname.split('/').filter(Boolean)
  if (kind !== 'in' || !segment) {
    return null
  }

  try {
    return normalizeLinkedinHandle(decodeURIComponent(segment))
  } catch {
    return null
  }
}
