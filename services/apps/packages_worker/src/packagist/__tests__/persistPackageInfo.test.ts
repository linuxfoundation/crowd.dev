import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getOrCreateRepoByUrl,
  updatePackagistPackageStats,
  upsertPackageMaintainers,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import type { NormalizedPackagistStats } from '../types'
import { persistPackagistPackageInfo } from '../upsertPackageInfo'

vi.mock('@crowd/data-access-layer/src/packages', () => ({
  updatePackagistPackageStats: vi.fn(),
  upsertPackageMaintainers: vi.fn().mockResolvedValue([]),
  getOrCreateRepoByUrl: vi.fn(),
  upsertPackageRepo: vi.fn().mockResolvedValue([]),
}))

const mockUpdate = vi.mocked(updatePackagistPackageStats)
const mockMaintainers = vi.mocked(upsertPackageMaintainers)
const mockRepoGet = vi.mocked(getOrCreateRepoByUrl)
const mockRepoLink = vi.mocked(upsertPackageRepo)

const qx = {} as QueryExecutor
const PURL = 'pkg:composer/monolog/monolog'

const stats: NormalizedPackagistStats = {
  name: 'monolog/monolog',
  description: 'logs',
  repositoryUrl: 'https://github.com/Seldaek/monolog',
  status: 'active',
  dependents: 42,
  downloadsTotal: 1000,
  downloadsMonthly: 300,
  maintainers: [{ username: 'seldaek', displayName: null, email: null, role: 'maintainer' }],
}

beforeEach(() => {
  vi.clearAllMocks()
  mockRepoGet.mockResolvedValue({ id: '55', changedFields: [] })
})

// Dynamic-endpoint persistence inside the metadata lane: packages fields + repo link for
// ALL packages; maintainers only for critical ones. Download rows are NOT written here —
// they belong to the dedicated downloads-30d/daily lanes.
describe('persistPackagistPackageInfo', () => {
  it('updates the packages row, links the repo, and writes maintainers for a critical package', async () => {
    mockUpdate.mockResolvedValue({
      id: '7',
      isCritical: true,
      changedFields: ['packages.description'],
    })

    const result = await persistPackagistPackageInfo(qx, PURL, stats)

    expect(mockUpdate).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({
        purl: PURL,
        description: 'logs',
        status: 'active',
        downloadsLast30d: 300,
        totalDownloads: 1000,
        dependentCount: 42,
      }),
    )
    // canonicalized (lowercased) url + coarse host, linked with the manifest-declared convention
    expect(mockRepoGet).toHaveBeenCalledWith(qx, 'https://github.com/seldaek/monolog', 'github')
    expect(mockRepoLink).toHaveBeenCalledWith(qx, '7', '55', 'declared', 0.8)
    expect(mockMaintainers).toHaveBeenCalledWith(qx, '7', stats.maintainers, 'packagist')
    expect(result.found).toBe(true)
    expect(result.changedFields).toContain('packages.description')
  })

  it('skips maintainers for a non-critical package but still links the repo', async () => {
    mockUpdate.mockResolvedValue({ id: '8', isCritical: false, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, stats)

    expect(mockMaintainers).not.toHaveBeenCalled()
    expect(mockRepoGet).toHaveBeenCalledWith(qx, 'https://github.com/seldaek/monolog', 'github')
    expect(mockRepoLink).toHaveBeenCalledWith(qx, '8', '55', 'declared', 0.8)
  })

  it('skips the repo link when the package declares no repository URL', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: true, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, { ...stats, repositoryUrl: null })

    expect(mockRepoGet).not.toHaveBeenCalled()
    expect(mockRepoLink).not.toHaveBeenCalled()
  })

  it('skips the repo link when the repository URL cannot be canonicalized', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: true, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, { ...stats, repositoryUrl: 'not-a-valid-url' })

    expect(mockRepoGet).not.toHaveBeenCalled()
    expect(mockRepoLink).not.toHaveBeenCalled()
  })

  it('folds repo changed-fields into the result for the audit log', async () => {
    mockUpdate.mockResolvedValue({
      id: '7',
      isCritical: false,
      changedFields: ['packages.description'],
    })
    mockRepoGet.mockResolvedValue({ id: '55', changedFields: ['repos.url', 'repos.host'] })
    mockRepoLink.mockResolvedValue(['package_repos.repo_id'])

    const result = await persistPackagistPackageInfo(qx, PURL, stats)

    expect(result.changedFields).toEqual(
      expect.arrayContaining([
        'packages.description',
        'repos.url',
        'repos.host',
        'package_repos.repo_id',
      ]),
    )
  })

  it('returns found=false and writes nothing else when the packages row is missing', async () => {
    mockUpdate.mockResolvedValue(null)

    const result = await persistPackagistPackageInfo(qx, PURL, stats)

    expect(result).toEqual({ found: false, changedFields: [] })
    expect(mockRepoGet).not.toHaveBeenCalled()
    expect(mockMaintainers).not.toHaveBeenCalled()
  })
})
