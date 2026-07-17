import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getPackagistMetadataDuePurls,
  insertDailyDownloads,
  insertPackagistPackages,
  logAuditFieldChanges,
  markPackagist30dProcessed,
  markPackagistDailyProcessed,
  markPackagistMetadataScanned,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  getPackagistMetadataBatch,
  ingestOnePackagist30dWindow,
  ingestOnePackagistDailyDownload,
  ingestOnePackagistMetadata,
  ingestPackagistItemsConcurrently,
  runPackagistPackageSeed,
} from '../activities'
import { persistPackagist30dWindow } from '../downloads'
import { expandComposerMetadata } from '../expandMetadata'
import { fetchPackagistP2, fetchPackagistStats } from '../fetchPackage'
import { fetchPackagistPackageList, parsePackagistPackageList } from '../listPackages'
import { INGEST_MAX_ATTEMPTS } from '../retryPolicy'
import { persistPackagistMetadata } from '../upsertMetadata'
import { persistPackagistPackageInfo } from '../upsertPackageInfo'

// Mock the heavy collaborators so we exercise the classification/retry/give-up logic.
vi.mock('../fetchPackage', () => ({
  fetchPackagistStats: vi.fn(),
  fetchPackagistP2: vi.fn(),
  buildPackagistUserAgent: vi.fn(() => 'ua'),
}))
vi.mock('../upsertPackageInfo', () => ({ persistPackagistPackageInfo: vi.fn() }))
vi.mock('../upsertMetadata', () => ({ persistPackagistMetadata: vi.fn() }))
vi.mock('../downloads', () => ({
  monthlyWindowFor: vi.fn(),
  persistPackagist30dWindow: vi.fn(),
}))
vi.mock('../expandMetadata', () => ({ expandComposerMetadata: vi.fn() }))
vi.mock('../listPackages', () => ({
  fetchPackagistPackageList: vi.fn(),
  parsePackagistPackageList: vi.fn(),
}))
vi.mock('@crowd/data-access-layer/src/packages', () => ({
  markPackagistMetadataScanned: vi.fn(),
  markPackagist30dProcessed: vi.fn(),
  markPackagistDailyProcessed: vi.fn(),
  getPackagistMetadataDuePurls: vi.fn(),
  getPackagist30dDuePurls: vi.fn(),
  getPackagistDailyDownloadsDue: vi.fn(),
  getCriticalPackagistPackageCount: vi.fn(),
  insertDailyDownloads: vi.fn().mockResolvedValue([]),
  insertPackagistPackages: vi.fn(),
  logAuditFieldChanges: vi.fn(),
}))

const mockFetchStats = vi.mocked(fetchPackagistStats)
const mockFetchP2 = vi.mocked(fetchPackagistP2)
const mockExpand = vi.mocked(expandComposerMetadata)
const mockPersistInfo = vi.mocked(persistPackagistPackageInfo)
const mockPersistMetadata = vi.mocked(persistPackagistMetadata)
const mockPersist30d = vi.mocked(persistPackagist30dWindow)
const mockDaily = vi.mocked(insertDailyDownloads)
const mockMarkMetadata = vi.mocked(markPackagistMetadataScanned)
const mockMark30d = vi.mocked(markPackagist30dProcessed)
const mockMarkDaily = vi.mocked(markPackagistDailyProcessed)
const mockAudit = vi.mocked(logAuditFieldChanges)
const mockFetchList = vi.mocked(fetchPackagistPackageList)
const mockParseList = vi.mocked(parsePackagistPackageList)
const mockSeedInsert = vi.mocked(insertPackagistPackages)

const qx = {} as QueryExecutor
const PURL = 'pkg:composer/monolog/monolog'
const RUN_DATE = '2026-07-15'

const statsJson = {
  package: { name: 'monolog/monolog', downloads: { total: 1000, monthly: 300, daily: 10 } },
}

beforeEach(() => {
  vi.clearAllMocks()
})

afterEach(() => {
  vi.useRealTimers()
})

// The merged metadata lane: one pass per package fetches BOTH registry endpoints —
// the dynamic one (package info, repos, maintainers) and p2 (versions, dependencies).
describe('ingestOnePackagistMetadata', () => {
  const candidate = { purl: PURL, metadataLastModified: 'Tue, 30 Jun 2026 00:00:00 GMT' }
  const minified = [{ version: '2.0.0' }]
  const expanded = [{ version: '2.0.0', version_normalized: '2.0.0.0' }]

  function happyMocks() {
    mockFetchStats.mockResolvedValue(statsJson as never)
    mockPersistInfo.mockResolvedValue({ found: true, changedFields: ['packages.description'] })
    mockFetchP2.mockResolvedValue({
      minifiedVersions: minified,
      lastModified: 'Wed, 01 Jul 2026 00:00:00 GMT',
    } as never)
    mockExpand.mockReturnValue(expanded as never)
    mockPersistMetadata.mockResolvedValue({
      found: true,
      changedFields: ['versions.number'],
      unresolvedDependencyTargets: 0,
    })
  }

  it('audits phase 1 immediately and phase 2 separately, then marks scanned with the fresh Last-Modified', async () => {
    happyMocks()

    await ingestOnePackagistMetadata(qx, candidate)

    expect(mockPersistInfo).toHaveBeenCalledWith(qx, PURL, expect.anything())
    expect(mockFetchP2).toHaveBeenCalledWith('monolog/monolog', 'Tue, 30 Jun 2026 00:00:00 GMT')
    expect(mockExpand).toHaveBeenCalledWith(minified)
    expect(mockPersistMetadata).toHaveBeenCalledWith(qx, PURL, expanded)
    // Two separate audit rows, not one merged call — phase 1 is logged as soon as it's
    // committed, before the p2 fetch (which can throw) ever runs.
    expect(mockAudit).toHaveBeenNthCalledWith(1, qx, 'packagist', PURL, ['packages.description'])
    expect(mockAudit).toHaveBeenNthCalledWith(2, qx, 'packagist', PURL, ['versions.number'])
    expect(mockMarkMetadata).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'success', attempts: 1 }),
      'Wed, 01 Jul 2026 00:00:00 GMT',
    )
  })

  it('records a p2 NOT_MODIFIED as success: info persisted, versions skipped', async () => {
    happyMocks()
    mockFetchP2.mockResolvedValue({ kind: 'NOT_MODIFIED' } as never)

    await ingestOnePackagistMetadata(qx, candidate)

    expect(mockPersistInfo).toHaveBeenCalled()
    expect(mockPersistMetadata).not.toHaveBeenCalled()
    // no fresh Last-Modified to replay when p2 wasn't refetched
    expect(mockMarkMetadata).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'success' }),
      null,
    )
  })

  it('gives up on a persistent dynamic 404 without touching p2', async () => {
    vi.useFakeTimers()
    mockFetchStats.mockResolvedValue({
      kind: 'NOT_FOUND',
      statusCode: 404,
      message: 'not found',
    } as never)

    const p = ingestOnePackagistMetadata(qx, candidate)
    await vi.runAllTimersAsync()
    await p

    expect(mockFetchStats).toHaveBeenCalledTimes(3)
    expect(mockFetchP2).not.toHaveBeenCalled()
    expect(mockPersistInfo).not.toHaveBeenCalled()
    expect(mockMarkMetadata).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'error', errorKind: 'NOT_FOUND', httpStatus: 404 }),
    )
  })

  it('throws on a transient dynamic result without marking scanned', async () => {
    mockFetchStats.mockResolvedValue({
      kind: 'TRANSIENT',
      statusCode: 500,
      message: 'HTTP 500',
    } as never)

    await expect(ingestOnePackagistMetadata(qx, candidate)).rejects.toThrow()
    expect(mockMarkMetadata).not.toHaveBeenCalled()
    expect(mockFetchStats).toHaveBeenCalledTimes(1)
  })

  it('gives up on a persistent p2 404 after fast retries and marks it scanned(error)', async () => {
    vi.useFakeTimers()
    mockFetchStats.mockResolvedValue(statsJson as never)
    mockPersistInfo.mockResolvedValue({ found: true, changedFields: [] })
    mockFetchP2.mockResolvedValue({
      kind: 'NOT_FOUND',
      statusCode: 404,
      message: 'not found',
    } as never)

    const p = ingestOnePackagistMetadata(qx, candidate)
    await vi.runAllTimersAsync()
    await p

    expect(mockPersistMetadata).not.toHaveBeenCalled()
    // p2-only failure must not bump metadata_last_run_at — phase 1 already succeeded,
    // but versions/deps never refreshed, so the package must stay due
    expect(mockMarkMetadata).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'error', errorKind: 'NOT_FOUND' }),
      undefined,
      false,
    )
  })

  it('audits phase-1 changes even when the p2 fetch gives up', async () => {
    // the dynamic-endpoint writes are committed by then — their audit rows must not be dropped
    vi.useFakeTimers()
    mockFetchStats.mockResolvedValue(statsJson as never)
    mockPersistInfo.mockResolvedValue({ found: true, changedFields: ['packages.description'] })
    mockFetchP2.mockResolvedValue({
      kind: 'NOT_FOUND',
      statusCode: 404,
      message: 'not found',
    } as never)

    const p = ingestOnePackagistMetadata(qx, candidate)
    await vi.runAllTimersAsync()
    await p

    expect(mockAudit).toHaveBeenCalledWith(qx, 'packagist', PURL, ['packages.description'])
    expect(mockMarkMetadata).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'error', errorKind: 'NOT_FOUND' }),
      undefined,
      false,
    )
  })

  it('throws on a transient p2 result without marking scanned, but still audits phase 1', async () => {
    mockFetchStats.mockResolvedValue(statsJson as never)
    mockPersistInfo.mockResolvedValue({ found: true, changedFields: ['packages.description'] })
    mockFetchP2.mockResolvedValue({ kind: 'TRANSIENT', message: 'HTTP 502' } as never)

    await expect(ingestOnePackagistMetadata(qx, candidate)).rejects.toThrow()
    expect(mockMarkMetadata).not.toHaveBeenCalled()
    // phase-1 writes are already committed when the throw happens — a retry re-runs
    // phase 1 idempotently and reports no changes, so this audit event can't be deferred
    expect(mockAudit).toHaveBeenCalledWith(qx, 'packagist', PURL, ['packages.description'])
  })
})

// The monthly downloads-30d lane: dynamic fetch, window row only.
describe('ingestOnePackagist30dWindow', () => {
  it('persists the observed rolling window, audits the change, and marks the run processed', async () => {
    mockFetchStats.mockResolvedValue(statsJson as never)
    mockPersist30d.mockResolvedValue(['downloads_last_30d.count', 'packages.downloads_last_30d'])

    await ingestOnePackagist30dWindow(qx, PURL, RUN_DATE)

    expect(mockPersist30d).toHaveBeenCalledWith(qx, PURL, 300, RUN_DATE)
    expect(mockAudit).toHaveBeenCalledWith(qx, 'packagist', PURL, [
      'downloads_last_30d.count',
      'packages.downloads_last_30d',
    ])
    expect(mockMark30d).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'success', attempts: 1 }),
    )
  })

  it('gives up on a persistent 404 and marks the run errored', async () => {
    vi.useFakeTimers()
    mockFetchStats.mockResolvedValue({
      kind: 'NOT_FOUND',
      statusCode: 404,
      message: 'not found',
    } as never)

    const p = ingestOnePackagist30dWindow(qx, PURL, RUN_DATE)
    await vi.runAllTimersAsync()
    await p

    expect(mockPersist30d).not.toHaveBeenCalled()
    expect(mockMark30d).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'error', errorKind: 'NOT_FOUND' }),
    )
  })

  it('throws on a transient result without marking processed', async () => {
    mockFetchStats.mockResolvedValue({ kind: 'TRANSIENT', message: 'HTTP 503' } as never)

    await expect(ingestOnePackagist30dWindow(qx, PURL, RUN_DATE)).rejects.toThrow()
    expect(mockMark30d).not.toHaveBeenCalled()
  })
})

// The daily downloads lane (critical slice): dynamic fetch, one downloads_daily row.
describe('ingestOnePackagistDailyDownload', () => {
  const candidate = { purl: PURL, packageId: '7' }

  it('inserts the daily row, audits the change, and marks the run processed', async () => {
    mockFetchStats.mockResolvedValue(statsJson as never)
    mockDaily.mockResolvedValue(['downloads_daily.date', 'downloads_daily.count'])

    await ingestOnePackagistDailyDownload(qx, candidate, RUN_DATE)

    expect(mockDaily).toHaveBeenCalledWith(qx, '7', [{ day: RUN_DATE, downloads: 10 }])
    expect(mockAudit).toHaveBeenCalledWith(qx, 'packagist', PURL, [
      'downloads_daily.date',
      'downloads_daily.count',
    ])
    expect(mockMarkDaily).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'success' }),
    )
  })

  it('marks success without inserting or auditing when the registry reports no daily count', async () => {
    mockFetchStats.mockResolvedValue({ package: { name: 'monolog/monolog' } } as never)

    await ingestOnePackagistDailyDownload(qx, candidate, RUN_DATE)

    expect(mockDaily).not.toHaveBeenCalled()
    expect(mockAudit).not.toHaveBeenCalled()
    expect(mockMarkDaily).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ status: 'success' }),
    )
  })

  it('throws on a transient result without marking processed', async () => {
    mockFetchStats.mockResolvedValue({ kind: 'TRANSIENT', message: 'HTTP 503' } as never)

    await expect(ingestOnePackagistDailyDownload(qx, candidate, RUN_DATE)).rejects.toThrow()
    expect(mockMarkDaily).not.toHaveBeenCalled()
  })
})

// Concurrency-bounded batch processing with the pypi give-up contract: transient failures
// rethrow while Temporal retries remain, then are given up per-item so the cursor advances.
describe('ingestPackagistItemsConcurrently', () => {
  const items = ['a', 'bad', 'c', 'd', 'e', 'f']
  const makeIngest = () =>
    vi.fn((item: string) =>
      item === 'bad' ? Promise.reject(new Error('transient')) : Promise.resolve(),
    )

  it('rethrows while Temporal retries remain and does not give up on any item', async () => {
    const ingest = makeIngest()
    const onGiveUp = vi.fn().mockResolvedValue(undefined)

    await expect(ingestPackagistItemsConcurrently(items, 1, 2, ingest, onGiveUp)).rejects.toThrow(
      'transient',
    )
    expect(onGiveUp).not.toHaveBeenCalled()
    // every item — including ones scheduled after 'bad' — must get a genuine attempt
    // this round; `attempt` tracks the whole batch, not each item, so a rethrow from
    // inside the concurrency pool must never starve later items of their own try.
    expect(ingest).toHaveBeenCalledTimes(6)
  })

  it('still attempts every later item when the FIRST item in the batch fails (sequential, deterministic)', async () => {
    const ingest = vi.fn((item: string) =>
      item === 'bad' ? Promise.reject(new Error('transient')) : Promise.resolve(),
    )
    const onGiveUp = vi.fn().mockResolvedValue(undefined)

    // concurrency=1 forces full sequencing: mapWithConcurrency awaits each item's
    // settlement (including the old code's rethrow-triggered `failed=true`) before
    // ever considering the next index, so this deterministically proves items after
    // the failing one still get scheduled instead of being starved.
    await expect(
      ingestPackagistItemsConcurrently(['bad', 'ok1', 'ok2'], 1, 1, ingest, onGiveUp),
    ).rejects.toThrow('transient')

    expect(ingest).toHaveBeenCalledTimes(3)
    expect(onGiveUp).not.toHaveBeenCalled()
  })

  it('on the final attempt gives up on the failing item and completes the rest', async () => {
    const ingest = makeIngest()
    const onGiveUp = vi.fn().mockResolvedValue(undefined)

    await expect(
      ingestPackagistItemsConcurrently(items, INGEST_MAX_ATTEMPTS, 2, ingest, onGiveUp),
    ).resolves.toBeUndefined()

    expect(ingest).toHaveBeenCalledTimes(6)
    expect(onGiveUp).toHaveBeenCalledTimes(1)
    expect(onGiveUp.mock.calls[0][0]).toBe('bad')
  })

  it('never runs more than `concurrency` ingests at once', async () => {
    let inFlight = 0
    let maxInFlight = 0
    const ingest = vi.fn(async () => {
      inFlight += 1
      maxInFlight = Math.max(maxInFlight, inFlight)
      await new Promise((resolve) => setTimeout(resolve, 5))
      inFlight -= 1
    })

    await ingestPackagistItemsConcurrently(items, 1, 2, ingest, vi.fn())

    expect(ingest).toHaveBeenCalledTimes(6)
    expect(maxInFlight).toBeLessThanOrEqual(2)
  })
})

// All-packages metadata scope: the env default drives due-selection's onlyCritical arg.
// Default is ALL packages (deps.dev has no Packagist data to fall back on); the env var
// narrows back to the critical slice.
describe('getPackagistMetadataBatch scope', () => {
  const mockMetadataDue = vi.mocked(getPackagistMetadataDuePurls)

  afterEach(() => {
    delete process.env.CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL
  })

  it('selects across ALL packagist packages by default', async () => {
    mockMetadataDue.mockResolvedValue([])

    await getPackagistMetadataBatch('', 50)

    expect(mockMetadataDue).toHaveBeenCalledWith(expect.anything(), '', 50, 7, false)
  })

  it('narrows to the critical slice when CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL=true', async () => {
    process.env.CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL = 'true'
    mockMetadataDue.mockResolvedValue([])

    await getPackagistMetadataBatch('', 50)

    expect(mockMetadataDue).toHaveBeenCalledWith(expect.anything(), '', 50, 7, true)
  })

  it('keeps the all-packages default when the env value is unrecognized', async () => {
    // a typo must not silently flip the sweep to critical-only
    process.env.CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL = 'ture'
    mockMetadataDue.mockResolvedValue([])

    await getPackagistMetadataBatch('', 50)

    expect(mockMetadataDue).toHaveBeenCalledWith(expect.anything(), '', 50, 7, false)
  })
})

// A1/A2 — the seed activity: one list fetch, parsed entries inserted, counts reported.
describe('runPackagistPackageSeed', () => {
  it('fetches the list once, seeds all parsed entries, and reports counts', async () => {
    const entries = [
      { vendor: 'a', name: 'x', purl: 'pkg:composer/a/x' },
      { vendor: 'b', name: 'y', purl: 'pkg:composer/b/y' },
      { vendor: 'c', name: 'z', purl: 'pkg:composer/c/z' },
    ]
    mockFetchList.mockResolvedValue({ packageNames: ['raw'] })
    mockParseList.mockReturnValue({ entries, invalid: 2 })
    mockSeedInsert.mockResolvedValue(3)

    const result = await runPackagistPackageSeed()

    expect(mockFetchList).toHaveBeenCalledTimes(1)
    const seeded = mockSeedInsert.mock.calls.flatMap((c) => c[1])
    expect(seeded).toEqual(entries)
    expect(result).toEqual({ discovered: 3, invalid: 2 })
  })

  it('throws when the list fetch fails so Temporal retries the seed', async () => {
    mockFetchList.mockResolvedValue({ kind: 'TRANSIENT', message: 'HTTP 502' })

    await expect(runPackagistPackageSeed()).rejects.toThrow()
    expect(mockSeedInsert).not.toHaveBeenCalled()
  })
})
