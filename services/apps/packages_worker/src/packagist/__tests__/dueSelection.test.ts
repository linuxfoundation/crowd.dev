import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getCriticalPackagistPackageCount,
  insertPackagistPackages,
  updatePackagistPackageStats,
} from '@crowd/data-access-layer/src/packages/packages'
import {
  getPackagist30dDuePurls,
  getPackagistDailyDownloadsDue,
  getPackagistMetadataDuePurls,
  markPackagist30dProcessed,
  markPackagistDailyProcessed,
  markPackagistMetadataScanned,
} from '@crowd/data-access-layer/src/packages/packagistPackageState'
import { upsertPackagistVersions } from '@crowd/data-access-layer/src/packages/versions'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

// Executes the real DAL functions against a capturing stand-in executor — no database.
function makeQx() {
  return {
    select: vi.fn().mockResolvedValue([]),
    selectOne: vi.fn().mockResolvedValue({ count: '0' }),
    selectOneOrNone: vi.fn().mockResolvedValue(null),
    result: vi.fn().mockResolvedValue(0),
    tx: vi.fn(),
  }
}
type FakeQx = ReturnType<typeof makeQx>
const asQx = (qx: FakeQx) => qx as unknown as QueryExecutor

let qx: FakeQx

beforeEach(() => {
  qx = makeQx()
})

// Metadata (merged enrichment) due-selection: keyset pagination, refresh window, and
// the stored Last-Modified for If-Modified-Since replay on the p2 fetch. `cutoff` is a
// pre-computed threshold (this run's fixed start time minus the refresh window), not a
// live NOW() — a keyset scan only visits each purl once per drain, so due-selection
// must stay anchored to one stable point in time for the whole run.
describe('getPackagistMetadataDuePurls', () => {
  const CUTOFF = '2026-07-08T00:00:00.000Z'

  it('returns purls with their stored Last-Modified, scoped by the fixed cutoff', async () => {
    qx.select.mockResolvedValue([
      { purl: 'pkg:composer/a/x', metadata_last_modified: 'Tue, 30 Jun 2026 00:00:00 GMT' },
      { purl: 'pkg:composer/b/y', metadata_last_modified: null },
    ])

    const candidates = await getPackagistMetadataDuePurls(asQx(qx), CUTOFF, '', 50, true)

    expect(candidates).toEqual([
      { purl: 'pkg:composer/a/x', metadataLastModified: 'Tue, 30 Jun 2026 00:00:00 GMT' },
      { purl: 'pkg:composer/b/y', metadataLastModified: null },
    ])
    const [sql, params] = qx.select.mock.calls[0]
    expect(sql).toMatch(/ecosystem = 'packagist'/)
    expect(sql).toMatch(/is_critical/)
    expect(sql).toMatch(/metadata_last_run_at/)
    expect(sql).toMatch(/ORDER BY\s+p\.purl/i)
    expect(params).toMatchObject({ cutoff: CUTOFF, batchSize: 50, onlyCritical: true })
  })

  it('selects across all packagist packages when onlyCritical is false (the all-packages default)', async () => {
    await getPackagistMetadataDuePurls(asQx(qx), CUTOFF, '', 50, false)

    const [sql, params] = qx.select.mock.calls[0]
    expect(sql).toMatch(/ecosystem = 'packagist'/)
    expect(params).toMatchObject({ onlyCritical: false })
  })
})

describe('markPackagistMetadataScanned', () => {
  it('upserts the metadata run result and stores a fresh Last-Modified when given', async () => {
    await markPackagistMetadataScanned(
      asQx(qx),
      'pkg:composer/a/x',
      { status: 'success', attempts: 1 },
      { metadataLastModified: 'Wed, 01 Jul 2026 00:00:00 GMT' },
    )

    const [sql, params] = qx.result.mock.calls[0]
    expect(sql).toMatch(/packagist_package_state/)
    expect(sql).toMatch(/metadata_run_result/)
    expect(sql).toMatch(/metadata_last_modified/)
    expect(sql).toMatch(/ON CONFLICT \(purl\)/)
    expect(params).toMatchObject({
      purl: 'pkg:composer/a/x',
      result: JSON.stringify({ status: 'success', attempts: 1 }),
      metadataLastModified: 'Wed, 01 Jul 2026 00:00:00 GMT',
    })
  })

  it('keeps the stored Last-Modified when none is given', async () => {
    await markPackagistMetadataScanned(asQx(qx), 'pkg:composer/a/x', {
      status: 'success',
      attempts: 1,
    })

    const [sql, params] = qx.result.mock.calls[0]
    expect(params).toMatchObject({ metadataLastModified: null })
    expect(sql).toMatch(/COALESCE/i)
  })
})

// Monthly downloads-30d lane: npm-style breadth — every package is due once per run
// (watermark older than the run's cutoff, or never run).
describe('getPackagist30dDuePurls / markPackagist30dProcessed', () => {
  it('selects packagist purls whose 30d watermark is older than the cutoff, keyset-paginated by purl', async () => {
    qx.select.mockResolvedValue([{ purl: 'pkg:composer/a/x' }])

    const purls = await getPackagist30dDuePurls(asQx(qx), '2026-07-01T03:53:00Z', '', 50)

    expect(purls).toEqual(['pkg:composer/a/x'])
    const [sql, params] = qx.select.mock.calls[0]
    expect(sql).toMatch(/ecosystem = 'packagist'/)
    expect(sql).toMatch(/downloads_30d_last_run_at/)
    expect(sql).toMatch(/p\.purl > \$\(afterPurl\)/)
    expect(sql).toMatch(/ORDER BY\s+p\.purl/i)
    expect(sql).toMatch(/LIMIT/i)
    expect(params).toMatchObject({ cutoff: '2026-07-01T03:53:00Z', afterPurl: '', batchSize: 50 })
  })

  it('bumps the 30d watermark with the run result', async () => {
    await markPackagist30dProcessed(asQx(qx), 'pkg:composer/a/x', {
      status: 'success',
      attempts: 1,
    })

    const [sql, params] = qx.result.mock.calls[0]
    expect(sql).toMatch(/packagist_package_state/)
    expect(sql).toMatch(/downloads_30d_last_run_at/)
    expect(sql).toMatch(/downloads_30d_run_result/)
    expect(sql).toMatch(/ON CONFLICT \(purl\)/)
    expect(params).toMatchObject({
      purl: 'pkg:composer/a/x',
      result: JSON.stringify({ status: 'success', attempts: 1 }),
    })
  })
})

// Daily downloads lane: critical slice only, cutoff watermark, returns the package id
// the downloads_daily insert needs.
describe('getPackagistDailyDownloadsDue / markPackagistDailyProcessed', () => {
  it('selects critical packagist packages due for a daily capture, keyset-paginated by purl', async () => {
    qx.select.mockResolvedValue([{ purl: 'pkg:composer/a/x', package_id: '7' }])

    const due = await getPackagistDailyDownloadsDue(asQx(qx), '2026-07-15T06:23:00Z', '', 50)

    expect(due).toEqual([{ purl: 'pkg:composer/a/x', packageId: '7' }])
    const [sql, params] = qx.select.mock.calls[0]
    expect(sql).toMatch(/ecosystem = 'packagist'/)
    expect(sql).toMatch(/is_critical/)
    expect(sql).toMatch(/daily_downloads_last_run_at/)
    expect(sql).toMatch(/p\.purl > \$\(afterPurl\)/)
    expect(sql).toMatch(/ORDER BY\s+p\.purl/i)
    expect(params).toMatchObject({ cutoff: '2026-07-15T06:23:00Z', afterPurl: '', batchSize: 50 })
  })

  it('bumps the daily watermark with the run result', async () => {
    await markPackagistDailyProcessed(asQx(qx), 'pkg:composer/a/x', {
      status: 'error',
      attempts: 3,
      errorKind: 'NOT_FOUND',
    })

    const [sql, params] = qx.result.mock.calls[0]
    expect(sql).toMatch(/daily_downloads_last_run_at/)
    expect(sql).toMatch(/daily_downloads_run_result/)
    expect(sql).toMatch(/ON CONFLICT \(purl\)/)
    expect(params).toMatchObject({ purl: 'pkg:composer/a/x' })
  })
})

// Seeding is insert-or-skip: existing rows are never clobbered.
describe('insertPackagistPackages', () => {
  it('inserts seed rows with packagist identity and skips existing purls', async () => {
    qx.result.mockResolvedValue(2)

    const inserted = await insertPackagistPackages(asQx(qx), [
      { purl: 'pkg:composer/a/x', vendor: 'a', name: 'x' },
      { purl: 'pkg:composer/b/y', vendor: 'b', name: 'y' },
    ])

    expect(inserted).toBe(2)
    const [sql, params] = qx.result.mock.calls[0]
    expect(sql).toMatch(/INSERT INTO packages/)
    expect(sql).toMatch(/'packagist'/)
    expect(sql).toMatch(/'packagist-registry'/)
    expect(sql).toMatch(/ON CONFLICT \(purl\) DO NOTHING/)
    expect(params).toMatchObject({
      purls: ['pkg:composer/a/x', 'pkg:composer/b/y'],
      vendors: ['a', 'b'],
      names: ['x', 'y'],
    })
  })

  it('is a no-op for an empty batch', async () => {
    expect(await insertPackagistPackages(asQx(qx), [])).toBe(0)
    expect(qx.result).not.toHaveBeenCalled()
  })
})

// The package-info update targets the packagist row and reports identity for scoping.
describe('updatePackagistPackageStats', () => {
  it('returns null when no packagist row matches the purl', async () => {
    expect(
      await updatePackagistPackageStats(asQx(qx), {
        purl: 'pkg:composer/a/x',
        description: null,
        declaredRepositoryUrl: null,
        repositoryUrl: null,
        status: 'active',
        totalDownloads: 2,
        dependentCount: 3,
      }),
    ).toBeNull()
    const [sql] = qx.selectOneOrNone.mock.calls[0]
    expect(sql).toMatch(/ecosystem = 'packagist'/)
  })

  it('maps the row to id/isCritical/changedFields', async () => {
    qx.selectOneOrNone.mockResolvedValue({
      id: '7',
      is_critical: true,
      changed_fields: ['packages.description'],
    })

    const result = await updatePackagistPackageStats(asQx(qx), {
      purl: 'pkg:composer/a/x',
      description: 'd',
      declaredRepositoryUrl: 'https://github.com/a/x',
      repositoryUrl: 'https://github.com/a/x',
      status: 'active',
      totalDownloads: 2,
      dependentCount: 3,
    })

    expect(result).toEqual({
      id: '7',
      isCritical: true,
      changedFields: ['packages.description'],
    })
  })
})

// The guard input: how many critical packagist packages exist.
describe('getCriticalPackagistPackageCount', () => {
  it('counts critical packagist packages', async () => {
    qx.selectOne.mockResolvedValue({ count: '12' })

    expect(await getCriticalPackagistPackageCount(asQx(qx))).toBe(12)
    const [sql] = qx.selectOne.mock.calls[0]
    expect(sql).toMatch(/ecosystem = 'packagist'/)
    expect(sql).toMatch(/is_critical/)
  })
})

// The stale-is_latest cleanup only ever flips rows OUTSIDE the upsert batch (batch rows
// already had is_latest set by the upsert CTE), so its row count must feed changedFields
// separately or those real changes never reach the audit log.
describe('upsertPackagistVersions is_latest cleanup audit', () => {
  const versions = [
    { number: '2.0.0', publishedAt: null, isLatest: true, isPrerelease: false, licenses: null },
  ]

  it("appends versions.is_latest when the cleanup cleared a stale row the CTE diff can't see", async () => {
    qx.selectOne.mockResolvedValue({ changed_fields: [], version_ids: [] })
    qx.result.mockResolvedValue(1)

    const { changedFields } = await upsertPackagistVersions(asQx(qx), '7', versions, '2.0.0')

    const [cleanupSql] = qx.result.mock.calls[0]
    expect(cleanupSql).toMatch(/is_latest = false/)
    expect(changedFields).toContain('versions.is_latest')
  })

  it('reports nothing extra when the cleanup cleared no rows, and never duplicates the CTE diff', async () => {
    qx.selectOne.mockResolvedValue({ changed_fields: ['versions.is_latest'], version_ids: [] })
    qx.result.mockResolvedValue(0)

    const noClear = await upsertPackagistVersions(asQx(qx), '7', versions, '2.0.0')
    expect(noClear.changedFields).toEqual(['versions.is_latest'])

    qx.result.mockResolvedValue(2)
    const cleared = await upsertPackagistVersions(asQx(qx), '7', versions, '2.0.0')
    expect(cleared.changedFields.filter((f) => f === 'versions.is_latest')).toHaveLength(1)
  })
})
