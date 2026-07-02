import { describe, expect, it } from 'vitest'

import {
  PYPI_EARLIEST,
  computeLast30dWindows,
  defaultDailyRange,
  utcFirstOfCurrentMonth,
} from '../pypiDownloadsDates'
import {
  PYPI_DOWNLOADS_30D_KIND,
  PYPI_DOWNLOADS_DAILY_KIND,
  buildPypiDownloads30dMergeSql,
  buildPypiDownloads30dSql,
  buildPypiDownloadsDailyMergeSql,
  buildPypiDownloadsDailySql,
} from '../pypiDownloadsSql'

// Note: PEP 503 name normalization is done in SQL (BQ `REGEXP_REPLACE(LOWER(file.project), …)`
// and the PG merge's `REGEXP_REPLACE(LOWER(p.name), …, 'g')`), asserted in the builder tests
// below — there is no TS-side normalizer (we don't push our package list into BigQuery).

// ── Criterion 2: monthly 30-day windows (npm-identical math) ───────────────────────────────
describe('computeLast30dWindows', () => {
  it('PYPI_EARLIEST is the 2018-07-26 Linehaul floor', () => {
    expect(PYPI_EARLIEST).toBe('2018-07-26')
  })

  it('with no fromDate returns only the latest bucket ending at upperEndDate', () => {
    const w = computeLast30dWindows(null, '2026-06-01')
    expect(w).toEqual([{ start: '2026-05-02', end: '2026-06-01', isLatest: true }])
  })

  it('enumerates a contiguous monthly series up to and including the latest', () => {
    const w = computeLast30dWindows('2026-04-10', '2026-06-01')
    expect(w.map((x) => x.end)).toEqual(['2026-04-01', '2026-05-01', '2026-06-01'])
    // end_date - 30 days for each bucket
    expect(w.map((x) => x.start)).toEqual(['2026-03-02', '2026-04-01', '2026-05-02'])
    // exactly one latest, and it is the final bucket
    expect(w.filter((x) => x.isLatest)).toEqual([
      { start: '2026-05-02', end: '2026-06-01', isLatest: true },
    ])
  })

  it('clamps the lower bound and start_date to PYPI_EARLIEST', () => {
    const w = computeLast30dWindows('2015-01-01', '2018-09-01')
    // 2018-07-01 bucket is skipped (its end precedes PYPI_EARLIEST)
    expect(w.map((x) => x.end)).toEqual(['2018-08-01', '2018-09-01'])
    // first bucket's start (would be 2018-07-02) is clamped up to the floor
    expect(w[0]).toEqual({ start: '2018-07-26', end: '2018-08-01', isLatest: false })
    expect(w[1].isLatest).toBe(true)
  })

  it('returns an empty array when fromDate is after upperEndDate', () => {
    expect(computeLast30dWindows('2026-07-01', '2026-06-01')).toEqual([])
  })
})

// ── Criterion 3: daily trailing window + first-of-month helper ─────────────────────────────
describe('defaultDailyRange', () => {
  it('defaults to a 2-day trailing re-scan window [today-2, today-1]', () => {
    expect(defaultDailyRange('2026-06-30')).toEqual({
      startDate: '2026-06-28',
      endDate: '2026-06-29',
    })
  })

  it('crosses month boundaries correctly', () => {
    expect(defaultDailyRange('2026-03-01')).toEqual({
      startDate: '2026-02-27',
      endDate: '2026-02-28',
    })
  })
})

describe('utcFirstOfCurrentMonth', () => {
  it('returns the 1st of the month containing the reference date', () => {
    expect(utcFirstOfCurrentMonth('2026-06-30')).toBe('2026-06-01')
    expect(utcFirstOfCurrentMonth('2026-01-15')).toBe('2026-01-01')
  })
})

// ── Criterion 4: 30d BigQuery aggregate query ─────────────────────────────────────────────
describe('buildPypiDownloads30dSql', () => {
  const sql = buildPypiDownloads30dSql({ startDate: '2026-05-02', endDate: '2026-06-01' })

  it('reads from the public file_downloads table', () => {
    expect(sql).toContain('bigquery-public-data.pypi.file_downloads')
  })

  it('filters the date range on the timestamp partition', () => {
    expect(sql).toContain('DATE(timestamp)')
    expect(sql).toContain('BETWEEN')
    expect(sql).toContain('2026-05-02')
    expect(sql).toContain('2026-06-01')
  })

  it('excludes bandersnatch mirror traffic while keeping NULL installers', () => {
    expect(sql).toContain("COALESCE(details.installer.name, '') <> 'bandersnatch'")
  })

  it('groups by the PEP 503-normalized project and counts downloads', () => {
    expect(sql).toContain("REGEXP_REPLACE(LOWER(file.project), r'[-_.]+', '-')")
    expect(sql).toMatch(/COUNT\(\*\)\s+AS\s+downloads/i)
    expect(sql).toContain('GROUP BY')
  })
})

// ── Criterion 5: daily BigQuery aggregate query ───────────────────────────────────────────
describe('buildPypiDownloadsDailySql', () => {
  const sql = buildPypiDownloadsDailySql({ startDate: '2026-06-01', endDate: '2026-06-03' })

  it('aggregates per project per day', () => {
    expect(sql).toContain('bigquery-public-data.pypi.file_downloads')
    expect(sql).toContain('DATE(timestamp) AS day')
    expect(sql).toContain("COALESCE(details.installer.name, '') <> 'bandersnatch'")
    expect(sql).toContain("REGEXP_REPLACE(LOWER(file.project), r'[-_.]+', '-')")
    expect(sql).toMatch(/COUNT\(\*\)\s+AS\s+downloads/i)
    expect(sql).toContain('GROUP BY project, day')
  })

  it('never pushes our package list into BigQuery — scoping to is_critical is the merge job', () => {
    expect(sql).not.toContain('UNNEST')
    expect(sql).not.toContain('is_critical')
  })
})

// ── Criterion 6: merge SQL builders ───────────────────────────────────────────────────────
describe('buildPypiDownloads30dMergeSql', () => {
  it('emits only the insert when not mirroring', () => {
    const stmts = buildPypiDownloads30dMergeSql({
      startDate: '2026-05-02',
      endDate: '2026-06-01',
      mirrorToPackages: false,
    })
    expect(stmts).toHaveLength(1)
    const insert = stmts[0]
    expect(insert).toContain('INSERT INTO downloads_last_30d')
    expect(insert).toContain('staging.pypi_downloads_30d_raw')
    expect(insert).toContain("p.ecosystem = 'pypi'")
    expect(insert).toContain('2026-05-02')
    expect(insert).toContain('2026-06-01')
    expect(insert).toContain('ON CONFLICT (purl, end_date) DO UPDATE')
    // PG-side normalization must use the 'g' flag to replace every separator run
    expect(insert).toContain("REGEXP_REPLACE(LOWER(p.name), '[-_.]+', '-', 'g')")
  })

  it('appends a packages mirror update when mirroring the latest window', () => {
    const stmts = buildPypiDownloads30dMergeSql({
      startDate: '2026-05-02',
      endDate: '2026-06-01',
      mirrorToPackages: true,
    })
    expect(stmts).toHaveLength(2)
    expect(stmts[1]).toContain('UPDATE packages')
    expect(stmts[1]).toContain('downloads_last_30d')
    expect(stmts[1]).toContain('IS DISTINCT FROM')
  })
})

describe('buildPypiDownloadsDailyMergeSql', () => {
  it('inserts into downloads_daily scoped to critical pypi packages', () => {
    const sql = buildPypiDownloadsDailyMergeSql()
    expect(sql).toContain('INSERT INTO downloads_daily')
    expect(sql).toContain('package_id')
    expect(sql).toContain('staging.pypi_downloads_daily_raw')
    expect(sql).toContain("p.ecosystem = 'pypi'")
    expect(sql).toContain('p.is_critical')
    expect(sql).toContain('ON CONFLICT (package_id, date) DO UPDATE')
    expect(sql).toContain("REGEXP_REPLACE(LOWER(p.name), '[-_.]+', '-', 'g')")
  })
})

// ── Criterion 7: job-kind registry ────────────────────────────────────────────────────────
describe('PyPI downloads job kinds', () => {
  it('exposes the two new kind identifiers', () => {
    expect(PYPI_DOWNLOADS_30D_KIND).toBe('pypi_downloads_30d')
    expect(PYPI_DOWNLOADS_DAILY_KIND).toBe('pypi_downloads_daily')
  })
})
