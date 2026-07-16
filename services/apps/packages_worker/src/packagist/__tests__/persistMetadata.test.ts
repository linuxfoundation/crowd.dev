import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getPackagistPackageIdsByNames,
  updatePackagistVersionAggregates,
  upsertPackagistVersions,
  upsertVersionDependencies,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import type { PackagistExpandedVersion } from '../types'
import { persistPackagistMetadata } from '../upsertMetadata'

vi.mock('@crowd/data-access-layer/src/packages', () => ({
  updatePackagistVersionAggregates: vi.fn(),
  upsertPackagistVersions: vi.fn(),
  getPackagistPackageIdsByNames: vi.fn(),
  upsertVersionDependencies: vi.fn().mockResolvedValue(0),
}))

const mockAggregates = vi.mocked(updatePackagistVersionAggregates)
const mockVersions = vi.mocked(upsertPackagistVersions)
const mockIds = vi.mocked(getPackagistPackageIdsByNames)
const mockDeps = vi.mocked(upsertVersionDependencies)

const qx = {} as QueryExecutor
const PURL = 'pkg:composer/monolog/monolog'

const expanded: PackagistExpandedVersion[] = [
  {
    version: '2.0.0',
    version_normalized: '2.0.0.0',
    time: '2024-01-01T00:00:00+00:00',
    license: ['MIT'],
    homepage: 'https://monolog.example.org',
    require: { php: '>=8.1', 'psr/log': '^3.0' },
    'require-dev': { 'phpunit/phpunit': '^10.5' },
  },
  {
    version: '1.0.0',
    version_normalized: '1.0.0.0',
    time: '2020-01-01T00:00:00+00:00',
    license: ['MIT'],
    require: { 'psr/log': '^1.0' },
  },
  { version: 'dev-main', version_normalized: 'dev-main' },
]

beforeEach(() => {
  vi.clearAllMocks()
  mockDeps.mockResolvedValue(0)
})

// C2 + C3 — persistence wiring: aggregates on packages, version rows, dependency edges
// resolved to existing packages rows only.
describe('persistPackagistMetadata', () => {
  it('upserts version rows (dev branches excluded), aggregates, and resolved dependency edges', async () => {
    mockAggregates.mockResolvedValue({ id: '9', changedFields: ['packages.latest_version'] })
    mockVersions.mockResolvedValue({
      changedFields: ['versions.number'],
      versionIds: [
        { number: '2.0.0', id: '21' },
        { number: '1.0.0', id: '20' },
      ],
    })
    mockIds.mockResolvedValue(
      new Map([
        ['psr/log', '44'],
        ['phpunit/phpunit', '45'],
      ]),
    )

    const result = await persistPackagistMetadata(qx, PURL, expanded)

    expect(mockAggregates).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({
        versionsCount: 2,
        latestVersion: '2.0.0',
        firstReleaseAt: '2020-01-01T00:00:00+00:00',
        latestReleaseAt: '2024-01-01T00:00:00+00:00',
        licenses: ['MIT'],
        homepage: 'https://monolog.example.org',
      }),
    )
    expect(mockVersions).toHaveBeenCalledWith(
      qx,
      '9',
      expect.arrayContaining([
        expect.objectContaining({ number: '2.0.0', isLatest: true }),
        expect.objectContaining({ number: '1.0.0', isLatest: false }),
      ]),
      '2.0.0',
    )
    expect(mockVersions.mock.calls[0][2]).toHaveLength(2)

    // platform 'php' never reaches resolution
    expect(mockIds).toHaveBeenCalledTimes(1)
    const requestedNames = mockIds.mock.calls[0][1]
    expect([...requestedNames].sort()).toEqual(['phpunit/phpunit', 'psr/log'])

    expect(mockDeps).toHaveBeenCalledTimes(1)
    const edges = mockDeps.mock.calls[0][1]
    expect(edges).toEqual(
      expect.arrayContaining([
        { packageId: '9', versionId: '21', dependsOnId: '44', constraint: '^3.0', kind: 'direct' },
        { packageId: '9', versionId: '21', dependsOnId: '45', constraint: '^10.5', kind: 'dev' },
        { packageId: '9', versionId: '20', dependsOnId: '44', constraint: '^1.0', kind: 'direct' },
      ]),
    )
    expect(edges).toHaveLength(3)

    expect(result.found).toBe(true)
    expect(result.unresolvedDependencyTargets).toBe(0)
  })

  it('skips and counts dependency targets that do not resolve to a packages row', async () => {
    mockAggregates.mockResolvedValue({ id: '9', changedFields: [] })
    mockVersions.mockResolvedValue({
      changedFields: [],
      versionIds: [
        { number: '2.0.0', id: '21' },
        { number: '1.0.0', id: '20' },
      ],
    })
    // phpunit/phpunit is unknown
    mockIds.mockResolvedValue(new Map([['psr/log', '44']]))

    const result = await persistPackagistMetadata(qx, PURL, expanded)

    const edges = mockDeps.mock.calls[0][1]
    expect(edges).toHaveLength(2)
    expect(edges.every((e) => e.dependsOnId === '44')).toBe(true)
    expect(result.unresolvedDependencyTargets).toBe(1)
  })

  it('returns found=false and writes nothing when the packages row is missing', async () => {
    mockAggregates.mockResolvedValue(null)

    const result = await persistPackagistMetadata(qx, PURL, expanded)

    expect(result).toEqual({ found: false, changedFields: [], unresolvedDependencyTargets: 0 })
    expect(mockVersions).not.toHaveBeenCalled()
    expect(mockIds).not.toHaveBeenCalled()
    expect(mockDeps).not.toHaveBeenCalled()
  })

  it('handles a dev-branches-only package: aggregates written, no version or dependency writes', async () => {
    mockAggregates.mockResolvedValue({ id: '9', changedFields: [] })

    const result = await persistPackagistMetadata(qx, PURL, [
      { version: 'dev-main', version_normalized: 'dev-main' },
    ])

    expect(mockAggregates).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ versionsCount: 0, latestVersion: null, homepage: null }),
    )
    expect(mockVersions).not.toHaveBeenCalled()
    expect(mockDeps).not.toHaveBeenCalled()
    expect(result.found).toBe(true)
  })

  it('strips NUL bytes from the homepage before writing (Postgres rejects them)', async () => {
    mockAggregates.mockResolvedValue({ id: '9', changedFields: [] })
    mockVersions.mockResolvedValue({ changedFields: [], versionIds: [] })

    await persistPackagistMetadata(qx, PURL, [
      { version: '1.0.0', version_normalized: '1.0.0.0', homepage: 'https://ex\0ample.org' },
    ])

    expect(mockAggregates).toHaveBeenCalledWith(
      qx,
      PURL,
      expect.objectContaining({ homepage: 'https://example.org' }),
    )
  })
})
