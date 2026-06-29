export const RAW_BASE = 'https://raw.githubusercontent.com'
export const GITHUB_API = 'https://api.github.com'

export interface FetchTextResult {
  status: number
  text: string | null
}

export async function fetchText(
  url: string,
  timeoutMs: number,
  headers: Record<string, string> = {},
): Promise<FetchTextResult> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetch(url, { headers, signal: controller.signal })
    if (res.status !== 200) return { status: res.status, text: null }
    return { status: 200, text: await res.text() }
  } finally {
    clearTimeout(timeoutId)
  }
}

export interface FetchJsonResult {
  status: number
  json: unknown | null
}

export async function fetchJson(
  url: string,
  timeoutMs: number,
  headers: Record<string, string> = {},
): Promise<FetchJsonResult> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetch(url, { headers, signal: controller.signal })
    if (res.status !== 200) return { status: res.status, json: null }
    return { status: 200, json: await res.json() }
  } finally {
    clearTimeout(timeoutId)
  }
}

export function githubAuthHeaders(token: string): Record<string, string> {
  return { Authorization: `bearer ${token}`, Accept: 'application/vnd.github+json' }
}

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/

export function isEmail(value: string): boolean {
  return EMAIL_RE.test(value.trim())
}

/** Returns the login if the URL is a bare github profile (github.com/<login>), else null. */
export function githubHandleFromUrl(value: string): string | null {
  const m = value.trim().match(/^https?:\/\/github\.com\/([^/\s]+)\/?$/i)
  return m ? m[1] : null
}
