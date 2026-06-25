import {
  NormalizedNuGetPackage,
  NormalizedNuGetVersion,
  NuGetCatalogEntry,
  NuGetRegistrationIndex,
  NuGetSearchItem,
} from './types'

function parseAuthors(authors: string | string[] | undefined): string[] {
  if (!authors) return []
  if (Array.isArray(authors)) return authors.filter(Boolean)
  return authors
    .split(',')
    .map((a) => a.trim())
    .filter(Boolean)
}

function isPrerelease(version: string): boolean {
  return version.includes('-')
}

function normalizeRepoUrl(url: string | undefined): string | null {
  if (!url) return null
  return url
    .trim()
    .replace(/\.git$/, '')
    .replace(/^git\+/, '')
    .replace(/^git:\/\//, 'https://')
    .replace(/^http:\/\/github\.com\//, 'https://github.com/')
}

function parseLicense(
  licenseExpression: string | undefined,
  licenseUrl: string | undefined,
): { licenses: string[] | null; licensesRaw: string | null } {
  if (licenseExpression) {
    return { licenses: [licenseExpression], licensesRaw: licenseExpression }
  }
  if (licenseUrl) {
    return { licenses: null, licensesRaw: licenseUrl }
  }
  return { licenses: null, licensesRaw: null }
}

export function normalizeNuGetPackage(
  packageId: string,
  searchResult: NuGetSearchItem | null,
  registration: NuGetRegistrationIndex,
): NormalizedNuGetPackage {
  const allLeaves = registration.items.flatMap((page) => page.items ?? [])
  const allEntries: NuGetCatalogEntry[] = allLeaves.map((leaf) => leaf.catalogEntry)

  const listedEntries = allEntries.filter((e) => e.listed !== false)
  const latestListedEntry =
    listedEntries.length > 0 ? listedEntries[listedEntries.length - 1] : null
  const latestEntry = allEntries.length > 0 ? allEntries[allEntries.length - 1] : null

  const latestVersion =
    searchResult?.version ?? latestListedEntry?.version ?? latestEntry?.version ?? null

  const latestEntry4License = latestListedEntry ?? latestEntry
  const { licenses, licensesRaw } = parseLicense(
    latestEntry4License?.licenseExpression,
    latestEntry4License?.licenseUrl,
  )

  const description =
    searchResult?.description || searchResult?.summary || latestListedEntry?.description || null

  const homepage = searchResult?.projectUrl || latestListedEntry?.projectUrl || null

  const declaredRepositoryUrl = latestEntry4License?.repository?.url
    ? latestEntry4License.repository.url
    : null
  const repositoryUrl = normalizeRepoUrl(declaredRepositoryUrl)

  const keywords = searchResult?.tags && searchResult.tags.length > 0 ? searchResult.tags : null

  let status: 'active' | 'deprecated' | 'unpublished'
  if (listedEntries.length === 0) {
    status = 'unpublished'
  } else if (latestListedEntry?.deprecation) {
    status = 'deprecated'
  } else {
    status = 'active'
  }

  const publishedDates = allEntries
    .filter((e) => e.published)
    .map((e) => new Date(e.published as string))
    .filter((d) => !isNaN(d.getTime()))
    .sort((a, b) => a.getTime() - b.getTime())

  const firstReleaseAt = publishedDates.length > 0 ? publishedDates[0] : null

  const latestEntry4Date = latestListedEntry ?? latestEntry
  const latestReleaseAt = latestEntry4Date?.published ? new Date(latestEntry4Date.published) : null

  const totalDownloads = searchResult?.totalDownloads ?? 0

  const owners = searchResult?.owners ?? []

  const authors = parseAuthors(searchResult?.authors ?? latestEntry4License?.authors)

  const searchVersionMap = new Map<string, number>()
  if (searchResult?.versions) {
    for (const v of searchResult.versions) {
      searchVersionMap.set(v.version.toLowerCase(), v.downloads)
    }
  }

  const versions: NormalizedNuGetVersion[] = allEntries.map((entry) => {
    const ver = entry.version
    const dlCount = searchVersionMap.get(ver.toLowerCase()) ?? null
    const { licenses: vLicenses } = parseLicense(entry.licenseExpression, entry.licenseUrl)
    return {
      number: ver,
      publishedAt: entry.published ? new Date(entry.published) : null,
      isLatest: ver === latestVersion,
      isPrerelease: isPrerelease(ver),
      isYanked: entry.listed === false,
      licenses: vLicenses,
      downloadCount: dlCount,
    }
  })

  return {
    description,
    homepage: homepage || null,
    declaredRepositoryUrl,
    repositoryUrl,
    licenses,
    licensesRaw,
    keywords,
    status,
    latestVersion,
    versionsCount: allEntries.length,
    firstReleaseAt,
    latestReleaseAt: latestReleaseAt && !isNaN(latestReleaseAt.getTime()) ? latestReleaseAt : null,
    totalDownloads,
    owners,
    authors,
    versions,
  }
}
