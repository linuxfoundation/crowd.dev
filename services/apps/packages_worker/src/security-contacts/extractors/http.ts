export const RAW_BASE = 'https://raw.githubusercontent.com'
export const GITHUB_API = 'https://api.github.com'

// crates.io rejects requests without a descriptive User-Agent (HTTP 403); harmless elsewhere.
// https://crates.io/policies#crawlers
export function registryHeaders(userAgent: string): Record<string, string> {
  return { 'User-Agent': userAgent }
}

// Genuinely-absent → null body; every other non-200 throws so transient failures (429/5xx/...)
// are treated as failures and the pipeline preserves existing data instead of wiping it.
const ABSENT_STATUSES = new Set([404, 410])

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
    if (res.status === 200) return { status: 200, text: await res.text() }
    if (ABSENT_STATUSES.has(res.status)) return { status: res.status, text: null }
    throw new Error(`fetchText ${url} failed: HTTP ${res.status}`)
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
    if (res.status === 200) return { status: 200, json: await res.json() }
    if (ABSENT_STATUSES.has(res.status)) return { status: res.status, json: null }
    throw new Error(`fetchJson ${url} failed: HTTP ${res.status}`)
  } finally {
    clearTimeout(timeoutId)
  }
}

export function githubAuthHeaders(token: string): Record<string, string> {
  return { Authorization: `bearer ${token}`, Accept: 'application/vnd.github+json' }
}

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/
const EMAIL_GLOBAL_RE = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g

export function isEmail(value: string): boolean {
  return EMAIL_RE.test(value.trim())
}

// Pulls all email addresses out of free-form strings like "Name <a@b.com>, Name2 <c@d.com>".
export function extractEmails(value: string): string[] {
  return value.match(EMAIL_GLOBAL_RE) ?? []
}

/** Returns the login if the URL is a bare github profile (github.com/<login>), else null. */
export function githubHandleFromUrl(value: string): string | null {
  const m = value.trim().match(/^https?:\/\/github\.com\/([^/\s]+)\/?$/i)
  return m ? m[1] : null
}
