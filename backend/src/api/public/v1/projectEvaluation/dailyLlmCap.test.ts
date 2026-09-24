import { afterEach, describe, expect, it } from 'vitest'

import { createDailyLlmCap, resolveDailyLlmCapMax } from './dailyLlmCap'

describe('createDailyLlmCap', () => {
  it('allows calls under the max and throws once the max is reached', () => {
    const reserve = createDailyLlmCap(() => 2)

    reserve('key-1')
    reserve('key-1')

    expect(() => reserve('key-1')).toThrow('Daily evaluation limit reached')
  })

  it('tracks each key independently', () => {
    const reserve = createDailyLlmCap(() => 1)

    reserve('key-1')

    expect(() => reserve('key-2')).not.toThrow()
    expect(() => reserve('key-1')).toThrow()
  })

  it('resets the counter on day rollover', () => {
    let day = '2026-09-24'
    const reserve = createDailyLlmCap(
      () => 1,
      () => new Date(`${day}T00:00:00.000Z`),
    )

    reserve('key-1')
    expect(() => reserve('key-1')).toThrow()

    day = '2026-09-25'
    expect(() => reserve('key-1')).not.toThrow()
  })
})

describe('resolveDailyLlmCapMax', () => {
  const originalEnv = { ...process.env }

  afterEach(() => {
    process.env = { ...originalEnv }
  })

  it('falls back to the built-in default when nothing is configured', () => {
    delete process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP
    delete process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES

    expect(resolveDailyLlmCapMax('any-key')).toBe(25)
  })

  it('uses the configured default for keys without an override', () => {
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP = '10'

    expect(resolveDailyLlmCapMax('any-key')).toBe(10)
  })

  it('applies a per-key override on top of the default', () => {
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP = '10'
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES = JSON.stringify({
      'projects-evaluation-worker': 500,
    })

    expect(resolveDailyLlmCapMax('projects-evaluation-worker')).toBe(500)
    expect(resolveDailyLlmCapMax('some-other-key')).toBe(10)
  })

  it('falls back to the default when the override JSON is malformed', () => {
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP = '10'
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES = 'not json'

    expect(resolveDailyLlmCapMax('any-key')).toBe(10)
  })

  it('ignores non-positive or non-integer override values', () => {
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP = '10'
    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP_OVERRIDES = JSON.stringify({
      negative: -5,
      float: 1.5,
    })

    expect(resolveDailyLlmCapMax('negative')).toBe(10)
    expect(resolveDailyLlmCapMax('float')).toBe(10)
  })
})
