import {
  getOrCreateRepoByUrl,
  upsertNpmFundingLinks,
  upsertNpmMaintainers,
  upsertPackageRepo,
  upsertPypiPackage,
  upsertPypiVersions,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'

import {
  classifyProjectUrls,
  collectPypiMaintainers,
  isPypiPrerelease,
  parseKeywords,
  pypiNameFromPurl,
  resolvePypiLicenses,
  stripNullBytesDeep,
} from './normalize'
import type { PyPiProject, PyPiReleaseFile } from './types'

interface PypiVersionRow {
  number: string
  publishedAt: string | null
  isLatest: boolean
  isPrerelease: boolean
  isYanked: boolean
  license: string | null
}

function fileUploadTimes(files: PyPiReleaseFile[]): string[] {
  return files
    .map((f) => f.upload_time_iso_8601)
    .filter((t): t is string => typeof t === 'string' && t.length > 0)
}

function minStr(arr: string[]): string | null {
  return arr.length ? arr.reduce((a, b) => (a < b ? a : b)) : null
}

function maxStr(arr: string[]): string | null {
  return arr.length ? arr.reduce((a, b) => (a > b ? a : b)) : null
}

export async function upsertProject(
  qx: QueryExecutor,
  project: PyPiProject,
  purl: string,
): Promise<{ purl: string; changedFields: string[] }> {
  const info = project.info
  stripNullBytesDeep(info)

  const name = info.name
  const status = info.yanked ? 'yanked' : 'active'
  const pypiName = pypiNameFromPurl(purl)
  const registryUrl =
    (typeof info.package_url === 'string' && info.package_url) ||
    `https://pypi.org/project/${pypiName}/`
  const description = info.summary?.trim() ? info.summary.trim() : null

  const { homepage, declaredRepositoryUrl, fundingLinks } = classifyProjectUrls(
    info.project_urls,
    info.home_page,
  )
  const repo = declaredRepositoryUrl ? canonicalizeRepoUrl(declaredRepositoryUrl) : null
  const { licenses, licensesRaw } = resolvePypiLicenses(info)
  const keywords = parseKeywords(info.keywords)
  const maintainers = collectPypiMaintainers(info)

  const releases = project.releases ?? {}
  const latestVersion = info.version ?? null
  const packageLicense = licenses[0] ?? null

  const allUploadTimes: string[] = []
  const versionRows: PypiVersionRow[] = []
  for (const [number, files] of Object.entries(releases)) {
    // A version whose files were all deleted has an empty array — no release artifact,
    // so skip it (it would carry no publish date and inflate the version count).
    if (!Array.isArray(files) || files.length === 0) continue
    const times = fileUploadTimes(files)
    allUploadTimes.push(...times)
    versionRows.push({
      number,
      publishedAt: minStr(times),
      isLatest: number === latestVersion,
      isPrerelease: isPypiPrerelease(number),
      isYanked: files.every((f) => f.yanked === true),
      license: packageLicense,
    })
  }

  const firstReleaseAt = minStr(allUploadTimes)
  const latestReleaseAt = maxStr(allUploadTimes)

  const changed = new Set<string>()

  await qx.tx(async (t) => {
    const { id: pkgId, changedFields: pkgChanged } = await upsertPypiPackage(t, {
      purl,
      namespace: null,
      name,
      status,
      registryUrl,
      description,
      homepage,
      declaredRepositoryUrl,
      repositoryUrl: repo?.url ?? null,
      licenses: licenses.length ? licenses : null,
      licensesRaw,
      keywords: keywords.length ? keywords : null,
      versionsCount: versionRows.length,
      latestVersion,
      firstReleaseAt,
      latestReleaseAt,
    })
    pkgChanged.forEach((f) => changed.add(f))

    if (repo) {
      const { id: repoId, changedFields: repoChanged } = await getOrCreateRepoByUrl(
        t,
        repo.url,
        repo.host,
      )
      repoChanged.forEach((f) => changed.add(f))
      const linkChanged = await upsertPackageRepo(t, pkgId, repoId, 'declared', 0.8)
      linkChanged.forEach((f) => changed.add(f))
    }

    if (versionRows.length > 0) {
      const verChanged = await upsertPypiVersions(t, pkgId, versionRows)
      verChanged.forEach((f) => changed.add(f))
    }

    if (maintainers.length > 0) {
      const mChanged = await upsertNpmMaintainers(t, pkgId, maintainers, 'pypi')
      mChanged.forEach((f) => changed.add(f))
    }

    if (fundingLinks.length > 0) {
      const fChanged = await upsertNpmFundingLinks(t, pkgId, fundingLinks)
      fChanged.forEach((f) => changed.add(f))
    }
  })

  return { purl, changedFields: Array.from(changed) }
}
