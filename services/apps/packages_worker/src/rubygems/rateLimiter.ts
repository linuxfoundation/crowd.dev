const MAX_RPS = Math.max(1, parseInt(process.env.RUBYGEMS_MAX_RPS ?? '10', 10))
const INTERVAL_MS = 1000 / MAX_RPS

let nextSlot = 0

export async function acquireRubyGemsSlot(): Promise<void> {
  const now = Date.now()
  const slot = Math.max(now, nextSlot)
  nextSlot = slot + INTERVAL_MS
  const wait = slot - now
  if (wait > 0) await new Promise((r) => setTimeout(r, wait))
}

export function parseRetryAfterMs(header: unknown): number {
  const FALLBACK_MS = 1000
  if (typeof header !== 'string') return FALLBACK_MS
  const seconds = Number(header)
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000)
  const date = new Date(header)
  if (!Number.isNaN(date.getTime())) return Math.max(0, date.getTime() - Date.now())
  return FALLBACK_MS
}
