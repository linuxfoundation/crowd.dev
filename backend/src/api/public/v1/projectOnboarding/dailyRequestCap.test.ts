import { afterEach, describe, expect, it } from 'vitest'

import {
  createDailyProjectOnboardingCap,
  resolveDailyProjectOnboardingCapMax,
} from './dailyRequestCap'

describe('createDailyProjectOnboardingCap', () => {
  it('allows calls under the max and throws once the max is reached', () => {
    const reserve = createDailyProjectOnboardingCap(() => 2)

    reserve('key-1')
    reserve('key-1')

    expect(() => reserve('key-1')).toThrow('Daily project onboarding request limit reached')
  })

  it('tracks each key independently', () => {
    const reserve = createDailyProjectOnboardingCap(() => 1)

    reserve('key-1')

    expect(() => reserve('key-2')).not.toThrow()
    expect(() => reserve('key-1')).toThrow()
  })

  it('resets the counter on day rollover', () => {
    let day = '2026-09-24'
    const reserve = createDailyProjectOnboardingCap(
      () => 1,
      () => new Date(`${day}T00:00:00.000Z`),
    )

    reserve('key-1')
    expect(() => reserve('key-1')).toThrow()

    day = '2026-09-25'
    expect(() => reserve('key-1')).not.toThrow()
  })
})

describe('resolveDailyProjectOnboardingCapMax', () => {
  const originalEnv = { ...process.env }

  afterEach(() => {
    process.env = { ...originalEnv }
  })

  it('falls back to the built-in default when nothing is configured', () => {
    delete process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP
    delete process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP_OVERRIDES

    expect(resolveDailyProjectOnboardingCapMax('any-key')).toBe(100)
  })

  it('uses the configured default for keys without an override', () => {
    process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP = '10'

    expect(resolveDailyProjectOnboardingCapMax('any-key')).toBe(10)
  })

  it('applies a per-key override on top of the default', () => {
    process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP = '10'
    process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP_OVERRIDES = JSON.stringify({
      'claude-bot': 500,
    })

    expect(resolveDailyProjectOnboardingCapMax('claude-bot')).toBe(500)
    expect(resolveDailyProjectOnboardingCapMax('some-other-key')).toBe(10)
  })
})
