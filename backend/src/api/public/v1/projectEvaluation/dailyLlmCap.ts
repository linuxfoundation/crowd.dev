import { RateLimitError } from '@crowd/common'

const DEFAULT_MAX = 25

function parseOverrides(raw: string | undefined): Record<string, number> {
  if (!raw) {
    return {}
  }

  try {
    const parsed = JSON.parse(raw)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return Object.fromEntries(
        Object.entries(parsed as Record<string, unknown>).filter(
          ([, value]) => typeof value === 'number' && Number.isSafeInteger(value) && value > 0,
        ),
      ) as Record<string, number>
    }
  } catch {
    // fall through to the empty default below
  }

  return {}
}

export function resolveDailyLlmCapMax(key: string): number {
  const configuredDefault = Number(process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP)
  const defaultMax =
    Number.isSafeInteger(configuredDefault) && configuredDefault > 0
      ? configuredDefault
      : DEFAULT_MAX

  const overrides = parseOverrides(process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES)
  return overrides[key] ?? defaultMax
}

// In-process, per-instance counter — a blast-radius limiter against runaway callers, not an
// accounting-grade budget. Resets on deploy and the effective ceiling scales with replica count.
export function createDailyLlmCap(
  resolveMax: (key: string) => number = resolveDailyLlmCapMax,
  today: () => Date = () => new Date(),
) {
  const counters = new Map<string, { day: string; count: number }>()

  return (key: string): void => {
    const day = today().toISOString().slice(0, 10)
    const entry = counters.get(key)
    const count = entry?.day === day ? entry.count : 0
    const max = resolveMax(key)

    if (count >= max) {
      throw new RateLimitError('Daily evaluation limit reached', { key, max })
    }

    counters.set(key, { day, count: count + 1 })
  }
}
