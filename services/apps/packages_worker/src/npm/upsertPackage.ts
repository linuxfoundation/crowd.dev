import {
  getOrCreateRepoByUrl,
  upsertNpmFundingLinks,
  upsertNpmMaintainers,
  upsertNpmPackage,
  upsertNpmVersions,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  collectMaintainers,
  extractRepo,
  isPrerelease,
  normalizeLicenses,
  parseNpmName,
  stripNullBytesDeep,
  versionLicense,
} from './normalize'
import type { FundingEntry, Packument } from './types'

// `purl` is the source-of-truth identifier taken from the packages row — the
// worker never generates purls from names. namespace/name are parsed from the
// packument's registry name only to populate those (mutable) columns.
export async function upsertPackage(
  qx: QueryExecutor,
  packument: Packument,
  purl: string,
): Promise<{ purl: string; changedFields: string[] }> {
  // Registry data can contain NUL bytes (e.g. mojibake descriptions) that Postgres
  // text columns reject; strip them before any field is persisted.
  stripNullBytesDeep(packument)

  const raw = packument.name
  const { namespace, name } = parseNpmName(raw)
  const licenses = normalizeLicenses(packument)
  const licensesRaw = typeof packument.license === 'string' ? packument.license : null
  const declaredRepositoryUrl = rawRepoUrl(packument)
  const repo = extractRepo(packument)
  const repositoryUrl = repo?.url ?? null
  const versionEntries = Object.entries(packument.versions)
  const time = packument.time ?? {}
  const latestVersion = packument['dist-tags']?.latest ?? null
  const firstReleaseAt = minTime(time)
  const latestReleaseAt = maxTime(time)
  const latestV = latestVersion ? packument.versions[latestVersion] : null
  const status = packument.unpublished
    ? 'unpublished'
    : latestV?.deprecated
      ? 'deprecated'
      : 'active'
  const registryUrl = `https://www.npmjs.com/package/${raw}`
  const fundingLinks = extractFundingLinks(latestV?.funding)
  const maintainers = collectMaintainers(packument)

  const changed = new Set<string>()

  await qx.tx(async (t) => {
    const { id: pkgId, changedFields: pkgChanged } = await upsertNpmPackage(t, {
      purl,
      namespace,
      name,
      status,
      registryUrl,
      description: packument.description ?? null,
      homepage: packument.homepage ?? null,
      declaredRepositoryUrl,
      repositoryUrl,
      licenses: licenses.length ? licenses : null,
      licensesRaw,
      keywords: packument.keywords?.length ? packument.keywords : null,
      distLatest: packument['dist-tags']?.latest ?? null,
      distNext: packument['dist-tags']?.next ?? null,
      distBeta: packument['dist-tags']?.beta ?? null,
      versionsCount: versionEntries.length,
      latestVersion,
      firstReleaseAt: firstReleaseAt ?? null,
      latestReleaseAt: latestReleaseAt ?? null,
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

    const verChanged = await upsertNpmVersions(
      t,
      pkgId,
      versionEntries.map(([number, v]) => ({
        number,
        publishedAt: time[number] ?? null,
        isLatest: number === latestVersion,
        isPrerelease: isPrerelease(number),
        license: versionLicense(v.license) ?? licenses[0] ?? null,
      })),
    )
    verChanged.forEach((f) => changed.add(f))

    if (maintainers.length > 0) {
      const mChanged = await upsertNpmMaintainers(t, pkgId, maintainers)
      mChanged.forEach((f) => changed.add(f))
    }

    if (fundingLinks.length > 0) {
      const fChanged = await upsertNpmFundingLinks(t, pkgId, fundingLinks)
      fChanged.forEach((f) => changed.add(f))
    }
  })

  return { purl, changedFields: Array.from(changed) }
}

function rawRepoUrl(packument: Packument): string | null {
  const repo = packument.repository
  if (!repo) return null
  return typeof repo === 'string' ? repo || null : repo.url || null
}

function extractFundingLinks(
  funding: FundingEntry | FundingEntry[] | undefined,
): Array<{ type?: string; url: string }> {
  if (!funding) return []
  const entries = Array.isArray(funding) ? funding : [funding]
  return entries.flatMap((e) => {
    if (typeof e === 'string') return e ? [{ url: e }] : []
    if (e?.url) return [{ type: e.type, url: e.url }]
    return []
  })
}

function minTime(time: Record<string, string>): string | null {
  const dates = Object.entries(time)
    .filter(([k]) => k !== 'created' && k !== 'modified')
    .map(([, v]) => v)
  return dates.length ? dates.reduce((a, b) => (a < b ? a : b)) : null
}

function maxTime(time: Record<string, string>): string | null {
  const dates = Object.entries(time)
    .filter(([k]) => k !== 'created' && k !== 'modified')
    .map(([, v]) => v)
  return dates.length ? dates.reduce((a, b) => (a > b ? a : b)) : null
}
