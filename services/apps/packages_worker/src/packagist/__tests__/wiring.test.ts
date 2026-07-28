import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

import { FETCHERS } from '../../security-contacts/extractors/registry/index'
import { PACKAGIST_CRONS } from '../schedule'

// D1 — wiring that unit tests can meaningfully pin down.

describe('package.json worker scripts', () => {
  // vitest runs with cwd at the package root; import.meta is unavailable under the CJS tsconfig.
  const pkg = JSON.parse(readFileSync('package.json', 'utf8')) as {
    scripts: Record<string, string>
  }

  it('declares start/dev scripts for the packagist worker', () => {
    expect(pkg.scripts['start:packagist-worker']).toContain('src/bin/packagist-worker.ts')
    expect(pkg.scripts['dev:packagist-worker']).toContain('src/bin/packagist-worker.ts')
  })
})

describe('schedule cadence', () => {
  it('defines the three packagist crons with minutes off :00 (crawler guideline)', () => {
    // metadata has no cron — the seed workflow chains it as a child on completion
    const crons = Object.entries(PACKAGIST_CRONS)
    expect(crons.map(([name]) => name).sort()).toEqual(['downloads30d', 'downloadsDaily', 'seed'])
    for (const [name, cron] of crons) {
      const minute = cron.split(' ')[0]
      expect(minute, `${name} cron minute`).toMatch(/^[1-9][0-9]?$/)
      expect(Number(minute), `${name} cron minute`).toBeLessThan(60)
    }
  })

  it('runs seed weekly, the 30d window capture monthly on the 1st, and daily downloads daily', () => {
    expect(PACKAGIST_CRONS.seed.split(' ')).toHaveLength(5)
    expect(PACKAGIST_CRONS.seed.split(' ')[4]).not.toBe('*')
    // monthly, anchored on the 1st so the observed rolling value sits on the boundary
    expect(PACKAGIST_CRONS.downloads30d.split(' ')[2]).toBe('1')
    expect(PACKAGIST_CRONS.downloads30d.split(' ')[4]).toBe('*')
    expect(PACKAGIST_CRONS.downloadsDaily.split(' ')[2]).toBe('*')
    expect(PACKAGIST_CRONS.downloadsDaily.split(' ')[4]).toBe('*')
  })
})

describe('security-contacts registry fetchers', () => {
  it('keys the Composer fetcher by the packagist ecosystem value', () => {
    expect(FETCHERS.packagist).toBeTypeOf('function')
    expect('composer' in FETCHERS).toBe(false)
  })
})
