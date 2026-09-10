import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getOrCreateRepoByUrl, upsertPypiPackage } from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import type { PyPiProject } from '../types'
import { upsertProject } from '../upsertProject'

vi.mock('@crowd/data-access-layer/src/packages', () => ({
  upsertPypiPackage: vi.fn(),
  upsertPypiVersions: vi.fn().mockResolvedValue([]),
  getOrCreateRepoByUrl: vi.fn(),
  upsertPackageRepo: vi.fn().mockResolvedValue([]),
  removeDeclaredPackageRepo: vi.fn().mockResolvedValue([]),
  upsertPackageMaintainers: vi.fn().mockResolvedValue([]),
  upsertNpmFundingLinks: vi.fn().mockResolvedValue([]),
}))

const mockUpsertPackage = vi.mocked(upsertPypiPackage)
const mockGetOrCreateRepo = vi.mocked(getOrCreateRepoByUrl)

const qx = {
  tx: vi.fn((cb: (t: QueryExecutor) => Promise<void>) => cb(qx)),
} as unknown as QueryExecutor
const PURL = 'pkg:pypi/flask'

const baseProject = (info: Partial<PyPiProject['info']>): PyPiProject => ({
  info: { name: 'flask', ...info },
})

beforeEach(() => {
  vi.clearAllMocks()
  mockUpsertPackage.mockResolvedValue({ id: '1', changedFields: [] })
  mockGetOrCreateRepo.mockResolvedValue({ id: '2', changedFields: [] })
})

describe('upsertProject — declaredRepositoryUrl', () => {
  it('stores the source candidate as declaredRepositoryUrl', async () => {
    await upsertProject(
      qx,
      baseProject({ project_urls: { Source: 'https://github.com/pallets/flask' } }),
      PURL,
    )

    expect(mockUpsertPackage).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ declaredRepositoryUrl: 'https://github.com/pallets/flask' }),
    )
  })

  it('does not surface a homepage-only fallback candidate as declaredRepositoryUrl', async () => {
    await upsertProject(
      qx,
      baseProject({ project_urls: { Homepage: 'https://github.com/psf/requests' } }),
      PURL,
    )

    expect(mockUpsertPackage).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ declaredRepositoryUrl: null }),
    )
  })

  it('does not surface a bug-tracker-only fallback candidate as declaredRepositoryUrl', async () => {
    await upsertProject(
      qx,
      baseProject({ project_urls: { 'Bug Tracker': 'https://github.com/foo/bar/issues' } }),
      PURL,
    )

    expect(mockUpsertPackage).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ declaredRepositoryUrl: null }),
    )
  })
})

describe('upsertProject — homepage repo candidate', () => {
  it('keeps a repo-looking project_urls.Homepage as a candidate even when info.home_page overrides packages.homepage', async () => {
    await upsertProject(
      qx,
      baseProject({
        home_page: 'https://docs.example.org',
        project_urls: { Homepage: 'https://github.com/org/repo' },
      }),
      PURL,
    )

    expect(mockUpsertPackage).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ homepage: 'https://docs.example.org' }),
    )
    expect(mockGetOrCreateRepo).toHaveBeenCalledWith(
      qx,
      'https://github.com/org/repo',
      expect.anything(),
    )
  })
})
