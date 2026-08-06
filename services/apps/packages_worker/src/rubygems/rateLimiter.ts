const MAX_RPS = Math.max(1, parseInt(process.env.RUBYGEMS_MAX_RPS ?? '10', 10))
const INTERVAL_MS = 1000 / MAX_RPS

let nextSlot = 0

export function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (!signal) return new Promise((r) => setTimeout(r, ms))
  const sig = signal
  return new Promise((resolve, reject) => {
    if (sig.aborted) {
      reject(sig.reason)
      return
    }
    function onAbort() {
      clearTimeout(timer)
      reject(sig.reason)
    }
    const timer = setTimeout(() => {
      sig.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    sig.addEventListener('abort', onAbort, { once: true })
  })
}

export async function acquireRubyGemsSlot(signal?: AbortSignal): Promise<void> {
  const now = Date.now()
  const slot = Math.max(now, nextSlot)
  nextSlot = slot + INTERVAL_MS
  const wait = slot - now
  if (wait > 0) await abortableSleep(wait, signal)
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
