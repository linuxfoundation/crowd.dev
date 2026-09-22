import { readFileSync } from 'fs'
import { join } from 'path'

import { describe, expect, test } from 'vitest'

import { trimReport } from './trimReport'
import type { ICheckResult } from './types'

function loadFixture(name: string): ICheckResult[] {
  return JSON.parse(readFileSync(join(__dirname, `__fixtures__/${name}-results.json`), 'utf-8'))
}

describe('trimReport', () => {
  test('maps each CheckResult to a row with the DB-facing field names', () => {
    const rows = trimReport([
      {
        id: 'llms-txt-exists',
        category: 'content-discoverability',
        status: 'fail',
        message: 'No llms.txt found',
        details: { candidateUrls: ['https://example.com/llms.txt'] },
      },
    ])

    expect(rows).toEqual([
      {
        checkId: 'llms-txt-exists',
        category: 'content-discoverability',
        status: 'fail',
        message: 'No llms.txt found',
        details: JSON.stringify({ candidateUrls: ['https://example.com/llms.txt'] }),
      },
    ])
  })

  test('maps a missing details object to null rather than the string "undefined"', () => {
    const rows = trimReport([
      { id: 'redirect-behavior', category: 'url-stability', status: 'pass', message: 'ok' },
    ])
    expect(rows[0].details).toBeNull()
  })

  test('caps details at 32 KB', () => {
    const huge = { blob: 'x'.repeat(100_000) }
    const rows = trimReport([
      {
        id: 'page-size-html',
        category: 'page-size',
        status: 'fail',
        message: 'too big',
        details: huge,
      },
    ])

    const byteLength = Buffer.byteLength(rows[0].details as string, 'utf-8')
    expect(byteLength).toBeLessThanOrEqual(32 * 1024)
    expect(byteLength).toBeGreaterThan(32 * 1024 - 100)
  })

  test('leaves details untouched when already under the 32 KB cap', () => {
    const small = { note: 'small enough' }
    const rows = trimReport([
      {
        id: 'cache-header-hygiene',
        category: 'observability',
        status: 'warn',
        message: '',
        details: small,
      },
    ])
    expect(rows[0].details).toBe(JSON.stringify(small))
  })

  test('preserves the count and id order of the input results', () => {
    const results = loadFixture('kyverno')
    const rows = trimReport(results)
    expect(rows).toHaveLength(results.length)
    expect(rows.map((r) => r.checkId)).toEqual(results.map((r) => r.id))
  })
})
