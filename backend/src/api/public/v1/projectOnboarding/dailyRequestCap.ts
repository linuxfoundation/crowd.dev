import { createDailyRequestCap, resolveDailyRequestCapMax } from '../shared/dailyRequestCap'

export function resolveDailyProjectOnboardingCapMax(key: string): number {
  return resolveDailyRequestCapMax(
    key,
    'CROWD_PROJECT_ONBOARDING_DAILY_CAP',
    'CROWD_PROJECT_ONBOARDING_DAILY_CAP_OVERRIDES',
  )
}

export function createDailyProjectOnboardingCap(
  resolveMax: (overrideKey: string) => number = resolveDailyProjectOnboardingCapMax,
  today: () => Date = () => new Date(),
) {
  return createDailyRequestCap('Daily project onboarding request limit reached', resolveMax, today)
}
