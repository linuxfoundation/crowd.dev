import type { CountWindow } from '@crowd/data-access-layer/src/connectors'

const DAY_MS = 24 * 60 * 60 * 1000
const SATURDAY = 6

export function previousValidationWeek(now: Date): Extract<CountWindow, { kind: 'range' }> {
  const startOfDay = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())
  const daysSinceSaturday = (now.getUTCDay() - SATURDAY + 7) % 7
  const to = startOfDay - daysSinceSaturday * DAY_MS
  return {
    kind: 'range',
    from: new Date(to - 7 * DAY_MS).toISOString(),
    to: new Date(to).toISOString(),
  }
}
