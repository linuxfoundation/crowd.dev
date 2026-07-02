// crates.io rejects requests without a descriptive User-Agent (HTTP 403); harmless elsewhere.
// https://crates.io/policies#crawlers
export function registryHeaders(userAgent: string): Record<string, string> {
  return { 'User-Agent': userAgent }
}

// Genuinely-absent / not-determinable → null body; every other non-200 throws so transient
// failures (5xx/...) are treated as failures and the pipeline preserves data instead of
// wiping it. 422 is included because GitHub's PVR endpoint returns it (per-repo, non-transient)
// when the flag can't be determined — that must read as "unknown", not block the whole repo.
const ABSENT_STATUSES = new Set([404, 410, 422])

// Registry rate-limit / overload responses: retried in-process (honoring Retry-After) so a brief
// throttle doesn't fail the extractor and cost the repo a whole refresh cadence.
const RATE_LIMIT_STATUSES = new Set([429, 503])
const MAX_RATE_LIMIT_RETRIES = 3

async function fetchWithRetry(
  url: string,
  timeoutMs: number,
  headers: Record<string, string>,
): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
    let res: Response
    try {
      res = await fetch(url, { headers, signal: controller.signal })
    } finally {
      clearTimeout(timeoutId)
    }
    if (!RATE_LIMIT_STATUSES.has(res.status) || attempt >= MAX_RATE_LIMIT_RETRIES) return res
    const retryAfterSec = parseInt(res.headers.get('retry-after') ?? '0', 10)
    const waitMs = retryAfterSec ? retryAfterSec * 1000 : Math.min(30_000, 1_000 * 2 ** attempt)
    await new Promise((r) => setTimeout(r, waitMs))
  }
}

export interface FetchTextResult {
  status: number
  text: string | null
}

export async function fetchText(
  url: string,
  timeoutMs: number,
  headers: Record<string, string> = {},
): Promise<FetchTextResult> {
  const res = await fetchWithRetry(url, timeoutMs, headers)
  if (res.status === 200) return { status: 200, text: await res.text() }
  if (ABSENT_STATUSES.has(res.status)) return { status: res.status, text: null }
  throw new Error(`fetchText ${url} failed: HTTP ${res.status}`)
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
  const res = await fetchWithRetry(url, timeoutMs, headers)
  if (res.status === 200) return { status: 200, json: await res.json() }
  if (ABSENT_STATUSES.has(res.status)) return { status: res.status, json: null }
  throw new Error(`fetchJson ${url} failed: HTTP ${res.status}`)
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
