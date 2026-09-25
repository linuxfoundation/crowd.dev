import { createDailyRequestCap, resolveDailyRequestCapMax } from '../shared/dailyRequestCap'

export function resolveDailyProjectCatalogCapMax(key: string): number {
  return resolveDailyRequestCapMax(
    key,
    'CROWD_PROJECT_CATALOG_DAILY_CAP',
    'CROWD_PROJECT_CATALOG_DAILY_CAP_OVERRIDES',
  )
}

export function createDailyProjectCatalogCap(
  resolveMax: (overrideKey: string) => number = resolveDailyProjectCatalogCapMax,
  today: () => Date = () => new Date(),
) {
  return createDailyRequestCap('Daily project catalog request limit reached', resolveMax, today)
}
