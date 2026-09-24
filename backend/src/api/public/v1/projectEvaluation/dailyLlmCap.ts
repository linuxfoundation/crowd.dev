import { RateLimitError } from '@crowd/common'

const DEFAULT_MAX = 100

function parseOverrides(raw: string | undefined): Map<string, number> {
  if (!raw) {
    return new Map()
  }

  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return new Map()
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return new Map()
  }

  return new Map(
    Object.entries(parsed as Record<string, unknown>).filter(
      (entry): entry is [string, number] =>
        typeof entry[1] === 'number' && Number.isSafeInteger(entry[1]) && entry[1] > 0,
    ),
  )
}

export function resolveDailyLlmCapMax(key: string): number {
  const configuredDefault = Number(process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP)
  const defaultMax =
    Number.isSafeInteger(configuredDefault) && configuredDefault > 0
      ? configuredDefault
      : DEFAULT_MAX

  const overrides = parseOverrides(process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES)
  return overrides.get(key) ?? defaultMax
}

// In-process, per-instance counter — a blast-radius limiter against runaway callers, not an
// accounting-grade budget. Resets on deploy and the effective ceiling scales with replica count.
export function createDailyLlmCap(
  resolveMax: (overrideKey: string) => number = resolveDailyLlmCapMax,
  today: () => Date = () => new Date(),
) {
  const counters = new Map<string, { day: string; count: number }>()

  // `counterKey` isolates the budget (must be unique per caller); `overrideKey` picks which
  // configured cap applies and may be shared by callers that aren't uniquely identifiable.
  return (counterKey: string, overrideKey: string = counterKey): void => {
    const day = today().toISOString().slice(0, 10)
    const entry = counters.get(counterKey)
    const count = entry?.day === day ? entry.count : 0
    const max = resolveMax(overrideKey)

    if (count >= max) {
      throw new RateLimitError('Daily evaluation limit reached', { counterKey, overrideKey, max })
    }

    counters.set(counterKey, { day, count: count + 1 })
  }
}
