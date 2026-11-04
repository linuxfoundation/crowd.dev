import { timeout } from '@crowd/common'

export interface ScorecardCheckPayload {
  name: string
  score: number
  reason: string | null
}

export interface ScorecardPayload {
  date: string
  score: number
  checks: ScorecardCheckPayload[]
}

export type ScorecardFailure = {
  kind: 'RATE_LIMIT' | 'TRANSIENT' | 'MALFORMED'
  statusCode?: number
  message: string
}

export type ScorecardFetch =
  | { kind: 'OK'; payload: ScorecardPayload }
  | { kind: 'NOT_FOUND' }
  | ScorecardFailure

export interface ScorecardApiConfig {
  apiBaseUrl: string
  fetchTimeoutMs: number
  onRetryWait?: () => void
}

const MAX_RETRIES = { RATE_LIMIT: 5, TRANSIENT: 2 } as const
const MAX_RETRY_WAIT_MS = 60_000

type RetryableKind = keyof typeof MAX_RETRIES

type Attempt =
  | ScorecardFetch
  | { kind: 'RETRY'; waitMs: number | null; giveUp: ScorecardFailure & { kind: RetryableKind } }

function backoffMs(attempt: number): number {
  return 1000 * 2 ** attempt
}

function retryAfterMs(res: Response): number | null {
  const retryAfterSec = parseInt(res.headers.get('retry-after') ?? '', 10)
  return Number.isNaN(retryAfterSec) ? null : retryAfterSec * 1000
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}

function parsePayload(body: unknown): ScorecardPayload | null {
  if (typeof body !== 'object' || body === null) return null
  const { date, score, checks } = body as Record<string, unknown>
  if (typeof date !== 'string' || typeof score !== 'number' || !Array.isArray(checks)) return null

  const parsedChecks: ScorecardCheckPayload[] = []
  for (const check of checks) {
    if (typeof check !== 'object' || check === null) return null
    const { name, score: checkScore, reason } = check as Record<string, unknown>
    if (typeof name !== 'string' || typeof checkScore !== 'number') return null
    parsedChecks.push({
      name,
      score: checkScore,
      reason: typeof reason === 'string' ? reason : null,
    })
  }
  return { date, score, checks: parsedChecks }
}

async function attemptFetch(url: string, timeoutMs: number): Promise<Attempt> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      return {
        kind: 'RETRY',
        waitMs: null,
        giveUp: { kind: 'TRANSIENT', message: `network error: ${errorMessage(e)}` },
      }
    }

    if (res.status === 429) {
      return {
        kind: 'RETRY',
        waitMs: retryAfterMs(res),
        giveUp: { kind: 'RATE_LIMIT', statusCode: 429, message: '429 after retries' },
      }
    }
    if (res.status === 404) return { kind: 'NOT_FOUND' }
    if (res.status >= 500) {
      return {
        kind: 'RETRY',
        waitMs: null,
        giveUp: {
          kind: 'TRANSIENT',
          statusCode: res.status,
          message: `${res.status} after retries`,
        },
      }
    }
    if (res.status !== 200) {
      return {
        kind: 'TRANSIENT',
        statusCode: res.status,
        message: `unexpected status ${res.status}`,
      }
    }

    let body: unknown
    try {
      body = await res.json()
    } catch {
      if (controller.signal.aborted) {
        return {
          kind: 'RETRY',
          waitMs: null,
          giveUp: { kind: 'TRANSIENT', statusCode: 200, message: 'body read timed out' },
        }
      }
      return { kind: 'MALFORMED', statusCode: 200, message: 'invalid json' }
    }
    const payload = parsePayload(body)
    if (!payload) return { kind: 'MALFORMED', statusCode: 200, message: 'unexpected payload shape' }
    return { kind: 'OK', payload }
  } finally {
    clearTimeout(timer)
  }
}

export async function fetchScorecard(
  projectPath: string,
  cfg: ScorecardApiConfig,
): Promise<ScorecardFetch> {
  const url = `${cfg.apiBaseUrl}/projects/${projectPath}`
  const retries: Record<RetryableKind, number> = { RATE_LIMIT: 0, TRANSIENT: 0 }

  for (;;) {
    const attempt = await attemptFetch(url, cfg.fetchTimeoutMs)
    if (attempt.kind !== 'RETRY') return attempt

    const kind = attempt.giveUp.kind
    if (retries[kind] >= MAX_RETRIES[kind]) return attempt.giveUp

    cfg.onRetryWait?.()
    await timeout(Math.min(attempt.waitMs ?? backoffMs(retries[kind]), MAX_RETRY_WAIT_MS))
    retries[kind]++
  }
}
