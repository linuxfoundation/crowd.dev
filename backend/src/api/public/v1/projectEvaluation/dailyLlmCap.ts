import { createDailyRequestCap, resolveDailyRequestCapMax } from '../shared/dailyRequestCap'

export function resolveDailyLlmCapMax(key: string): number {
  return resolveDailyRequestCapMax(
    key,
    'CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP',
    'CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES',
  )
}

export function createDailyLlmCap(
  resolveMax: (overrideKey: string) => number = resolveDailyLlmCapMax,
  today: () => Date = () => new Date(),
) {
  return createDailyRequestCap('Daily evaluation limit reached', resolveMax, today)
}
