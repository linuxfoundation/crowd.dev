import { afterEach, describe, expect, it } from 'vitest'

import { createDailyProjectCatalogCap, resolveDailyProjectCatalogCapMax } from './dailyRequestCap'

describe('createDailyProjectCatalogCap', () => {
  it('allows calls under the max and throws once the max is reached', () => {
    const reserve = createDailyProjectCatalogCap(() => 2)

    reserve('key-1')
    reserve('key-1')

    expect(() => reserve('key-1')).toThrow('Daily project catalog request limit reached')
  })

  it('tracks each key independently', () => {
    const reserve = createDailyProjectCatalogCap(() => 1)

    reserve('key-1')

    expect(() => reserve('key-2')).not.toThrow()
    expect(() => reserve('key-1')).toThrow()
  })

  it('resets the counter on day rollover', () => {
    let day = '2026-09-24'
    const reserve = createDailyProjectCatalogCap(
      () => 1,
      () => new Date(`${day}T00:00:00.000Z`),
    )

    reserve('key-1')
    expect(() => reserve('key-1')).toThrow()

    day = '2026-09-25'
    expect(() => reserve('key-1')).not.toThrow()
  })
})

describe('resolveDailyProjectCatalogCapMax', () => {
  const originalEnv = { ...process.env }

  afterEach(() => {
    process.env = { ...originalEnv }
  })

  it('falls back to the built-in default when nothing is configured', () => {
    delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP
    delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP_OVERRIDES

    expect(resolveDailyProjectCatalogCapMax('any-key')).toBe(100)
  })

  it('uses the configured default for keys without an override', () => {
    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '10'

    expect(resolveDailyProjectCatalogCapMax('any-key')).toBe(10)
  })

  it('applies a per-key override on top of the default', () => {
    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '10'
    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP_OVERRIDES = JSON.stringify({
      'claude-bot': 500,
    })

    expect(resolveDailyProjectCatalogCapMax('claude-bot')).toBe(500)
    expect(resolveDailyProjectCatalogCapMax('some-other-key')).toBe(10)
  })
})
