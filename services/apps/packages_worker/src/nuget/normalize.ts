import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'

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

// NuGet stamps unlisted versions with 1900-01-01T00:00:00Z as a sentinel — treat as absent.
function parsePublishedDate(published: string | undefined): Date | null {
  if (!published) return null
  const date = new Date(published)
  return !isNaN(date.getTime()) && date.getUTCFullYear() > 1900 ? date : null
}

const SCM_HOSTS = ['github.com', 'gitlab.com', 'bitbucket.org']

function isScmUrl(url: string | undefined): boolean {
  if (!url) return false
  try {
    return SCM_HOSTS.some((h) => new URL(url).hostname.endsWith(h))
  } catch {
    return false
  }
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

  // Scan all entries (prefer latest listed, then any) for a nuspec <repository> url.
  // Fall back to a SCM-shaped projectUrl/homepage when no nuspec repository is present.
  const entriesForRepo = latestListedEntry
    ? [
        latestListedEntry,
        ...listedEntries.slice(0, -1).reverse(),
        ...(latestEntry ? [latestEntry] : []),
      ]
    : [...allEntries].reverse()
  const nuspecRepoUrl = entriesForRepo.find((e) => e.repository?.url)?.repository?.url
  const declaredRepositoryUrl = nuspecRepoUrl ?? null
  const repo =
    (nuspecRepoUrl ? canonicalizeRepoUrl(nuspecRepoUrl) : null) ??
    (isScmUrl(homepage) ? canonicalizeRepoUrl(homepage) : null)

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
    .map((e) => parsePublishedDate(e.published))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => a.getTime() - b.getTime())

  const firstReleaseAt = publishedDates.length > 0 ? publishedDates[0] : null

  const latestEntry4Date = latestListedEntry ?? latestEntry
  const latestReleaseAt = parsePublishedDate(latestEntry4Date?.published)

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
      publishedAt: parsePublishedDate(entry.published),
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
    repo,
    licenses,
    licensesRaw,
    keywords,
    status,
    latestVersion,
    versionsCount: allEntries.length,
    firstReleaseAt,
    latestReleaseAt,
    totalDownloads,
    owners,
    authors,
    versions,
  }
}
