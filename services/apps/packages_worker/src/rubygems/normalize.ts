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

export function normalizeRubyGemsPackage(doc: RubyGemsGemResponse): NormalizedRubyGemsPackage {
  const licenses = doc.licenses && doc.licenses.length > 0 ? doc.licenses : null
  return {
    description: nonEmpty(doc.info),
    homepage: nonEmpty(doc.homepage_uri),
    declaredRepositoryUrl: nonEmpty(doc.source_code_uri),
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
    licenses: item.licenses && item.licenses.length > 0 ? item.licenses : null,
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
