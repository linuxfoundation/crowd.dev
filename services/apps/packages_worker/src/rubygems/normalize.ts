import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'

import {
  NormalizedRubyGemsOwner,
  NormalizedRubyGemsPackage,
  NormalizedRubyGemsVersion,
  RubyGemsGemResponse,
  RubyGemsOwner,
  RubyGemsVersionItem,
} from './types'

function nonEmpty(value: string | null | undefined): string | null {
  if (!value) return null
  const trimmed = value.trim()
  return trimmed === '' ? null : trimmed
}

function cleanLicenses(raw: (string | null)[] | null | undefined): string[] | null {
  const cleaned = raw?.filter((l): l is string => !!l && l.trim() !== '')
  return cleaned && cleaned.length > 0 ? cleaned : null
}

export function normalizeRubyGemsPackage(doc: RubyGemsGemResponse): NormalizedRubyGemsPackage {
  const licenses = cleanLicenses(doc.licenses)
  const declaredRepositoryUrl = nonEmpty(doc.source_code_uri)
  return {
    description: nonEmpty(doc.info),
    homepage: nonEmpty(doc.homepage_uri),
    declaredRepositoryUrl,
    repo: declaredRepositoryUrl ? canonicalizeRepoUrl(declaredRepositoryUrl) : null,
    licenses,
    licensesRaw: licenses ? licenses.join(', ') : null,
    latestVersion: nonEmpty(doc.version),
    totalDownloads: doc.downloads ?? 0,
  }
}

function parseCreatedAt(value: string | undefined): Date | null {
  if (!value) return null
  const date = new Date(value)
  return isNaN(date.getTime()) ? null : date
}

export function normalizeRubyGemsVersions(
  items: RubyGemsVersionItem[],
): NormalizedRubyGemsVersion[] {
  return items.map((item) => ({
    number: item.number,
    publishedAt: parseCreatedAt(item.created_at),
    isPrerelease: item.prerelease ?? false,
    licenses: cleanLicenses(item.licenses),
  }))
}

export function pickLatestRubyGemsVersion(
  versions: NormalizedRubyGemsVersion[],
): NormalizedRubyGemsVersion | null {
  if (versions.length === 0) return null
  const stable = versions.find((v) => !v.isPrerelease)
  return stable ?? versions[0]
}

export function normalizeRubyGemsOwners(owners: RubyGemsOwner[]): NormalizedRubyGemsOwner[] {
  return owners
    .filter((o): o is RubyGemsOwner & { handle: string } => !!o.handle && o.handle.trim() !== '')
    .map((o) => ({ username: o.handle, email: nonEmpty(o.email) }))
}
