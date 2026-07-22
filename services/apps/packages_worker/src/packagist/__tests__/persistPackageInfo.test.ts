import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getOrCreateRepoByUrl,
  logAuditFieldChanges,
  removeDeclaredPackageRepo,
  updatePackagistPackageStats,
  upsertPackageMaintainers,
  upsertPackageRepoPreserveProvenance,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import type { NormalizedPackagistStats } from '../types'
import { persistPackagistPackageInfo } from '../upsertPackageInfo'

vi.mock('@crowd/data-access-layer/src/packages', () => ({
  updatePackagistPackageStats: vi.fn(),
  upsertPackageMaintainers: vi.fn().mockResolvedValue([]),
  getOrCreateRepoByUrl: vi.fn(),
  upsertPackageRepoPreserveProvenance: vi.fn().mockResolvedValue([]),
  removeDeclaredPackageRepo: vi.fn().mockResolvedValue([]),
  logAuditFieldChanges: vi.fn(),
}))

const mockUpdate = vi.mocked(updatePackagistPackageStats)
const mockMaintainers = vi.mocked(upsertPackageMaintainers)
const mockRepoGet = vi.mocked(getOrCreateRepoByUrl)
const mockRepoLink = vi.mocked(upsertPackageRepoPreserveProvenance)
const mockRepoRemove = vi.mocked(removeDeclaredPackageRepo)
const mockAudit = vi.mocked(logAuditFieldChanges)

const qx = {
  tx: vi.fn((cb: (t: QueryExecutor) => Promise<void>) => cb(qx)),
} as unknown as QueryExecutor
const PURL = 'pkg:composer/monolog/monolog'

const stats: NormalizedPackagistStats = {
  name: 'monolog/monolog',
  description: 'logs',
  repositoryUrl: 'https://github.com/Seldaek/monolog',
  status: 'active',
  dependents: 42,
  downloadsTotal: 1000,
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
    mockMaintainers.mockResolvedValue(['maintainers.display_name'])

    const result = await persistPackagistPackageInfo(qx, PURL, stats)

    expect(mockUpdate).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({
        purl: PURL,
        description: 'logs',
        status: 'active',
        totalDownloads: 1000,
        dependentCount: 42,
      }),
    )
    // packages.downloads_last_30d belongs exclusively to the dedicated downloads-30d
    // lane's boundary-anchored snapshot — the metadata lane must never touch it
    expect(mockUpdate.mock.calls[0][1]).not.toHaveProperty('downloadsLast30d')
    // canonicalized (lowercased) url + coarse host, linked with the manifest-declared convention
    expect(mockRepoGet).toHaveBeenCalledWith(qx, 'https://github.com/seldaek/monolog', 'github')
    expect(mockRepoLink).toHaveBeenCalledWith(qx, '7', '55', 'declared', 0.8)
    // any stale 'declared' link pointing at a different repo is pruned in the same pass
    expect(mockRepoRemove).toHaveBeenCalledWith(qx, '7', '55')
    expect(mockMaintainers).toHaveBeenCalledWith(qx, '7', stats.maintainers, 'packagist')
    expect(result.found).toBe(true)
    expect(result.changedFields).toContain('packages.description')
    // maintainer changes must reach the audit log too, matching the npm/pypi paths
    expect(result.changedFields).toContain('maintainers.display_name')
    // audited atomically inside the same transaction as the writes above
    expect(mockAudit).toHaveBeenCalledWith(
      qx,
      'packagist',
      PURL,
      expect.arrayContaining(['packages.description', 'maintainers.display_name']),
    )
  })

  it('skips maintainers for a non-critical package but still links the repo', async () => {
    mockUpdate.mockResolvedValue({ id: '8', isCritical: false, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, stats)

    expect(mockMaintainers).not.toHaveBeenCalled()
    expect(mockRepoGet).toHaveBeenCalledWith(qx, 'https://github.com/seldaek/monolog', 'github')
    expect(mockRepoLink).toHaveBeenCalledWith(qx, '8', '55', 'declared', 0.8)
  })

  it('prunes a stale declared link when the repository URL switches to a different repo', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: false, changedFields: [] })
    mockRepoGet.mockResolvedValue({ id: '99', changedFields: [] })
    mockRepoRemove.mockResolvedValue(['package_repos.repo_id'])

    const result = await persistPackagistPackageInfo(qx, PURL, stats)

    expect(mockRepoLink).toHaveBeenCalledWith(qx, '7', '99', 'declared', 0.8)
    // old link (some other repo_id) removed, new one (99) kept
    expect(mockRepoRemove).toHaveBeenCalledWith(qx, '7', '99')
    expect(result.changedFields).toContain('package_repos.repo_id')
  })

  it('reconciles a maintainer list that dropped to empty for a critical package', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: true, changedFields: [] })
    mockMaintainers.mockResolvedValue(['package_maintainers.maintainer_id'])

    const result = await persistPackagistPackageInfo(qx, PURL, { ...stats, maintainers: [] })

    // upsertPackageMaintainers has replace/delete semantics — it must still run with an
    // empty list so stale rows for maintainers no longer reported get removed.
    expect(mockMaintainers).toHaveBeenCalledWith(qx, '7', [], 'packagist')
    expect(result.changedFields).toContain('package_maintainers.maintainer_id')
  })

  it('skips the repo link and clears any previously-declared one when there is no repository URL', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: true, changedFields: [] })
    mockRepoRemove.mockResolvedValue(['package_repos.repo_id'])

    const result = await persistPackagistPackageInfo(qx, PURL, { ...stats, repositoryUrl: null })

    expect(mockUpdate).toHaveBeenCalledWith(qx, expect.objectContaining({ repositoryUrl: null }))
    expect(mockRepoGet).not.toHaveBeenCalled()
    expect(mockRepoLink).not.toHaveBeenCalled()
    expect(mockRepoRemove).toHaveBeenCalledWith(qx, '7')
    expect(result.changedFields).toContain('package_repos.repo_id')
  })

  it('skips the repo link and clears the stale one when the repository URL cannot be canonicalized', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: true, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, { ...stats, repositoryUrl: 'not-a-valid-url' })

    expect(mockRepoGet).not.toHaveBeenCalled()
    expect(mockRepoLink).not.toHaveBeenCalled()
    expect(mockRepoRemove).toHaveBeenCalledWith(qx, '7')
  })

  it('does not trust a canonicalized host outside the SCM allowlist (wiki/issue-tracker/registry URLs)', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: true, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, {
      ...stats,
      // canonicalizeRepoUrl resolves this to a real { url, host: 'other' } pair —
      // it's not a parse failure, just not a verified SCM host.
      repositoryUrl: 'https://www.mediawiki.org/wiki/Extension:Nuke',
    })

    expect(mockUpdate).toHaveBeenCalledWith(qx, expect.objectContaining({ repositoryUrl: null }))
    expect(mockRepoGet).not.toHaveBeenCalled()
    expect(mockRepoLink).not.toHaveBeenCalled()
    expect(mockRepoRemove).toHaveBeenCalledWith(qx, '7')
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
    expect(mockAudit).not.toHaveBeenCalled()
  })

  it('strips NUL bytes from the description before writing (Postgres rejects them)', async () => {
    mockUpdate.mockResolvedValue({ id: '7', isCritical: false, changedFields: [] })

    await persistPackagistPackageInfo(qx, PURL, {
      ...stats,
      description: 'Esta es una descripci n random',
    })

    expect(mockUpdate).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ description: 'Esta es una descripcin random' }),
    )
  })
})
