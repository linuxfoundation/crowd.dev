import type { PackumentVersion } from '../npm/types'

// The shared Packument/PackumentVersion types (services/apps/packages_worker/src/npm/types.ts)
// only declare the fields the ingest pipeline reads. Blast-radius additionally needs the
// dependency maps and tarball URL, which npm's registry always returns but the shared type
// doesn't model — extending it locally here rather than widening the shared type for one caller.
export interface NpmVersionManifest extends PackumentVersion {
  dependencies?: Record<string, string>
  peerDependencies?: Record<string, string>
  optionalDependencies?: Record<string, string>
  dist?: { tarball?: string }
}

export function asNpmVersionManifest(version: PackumentVersion): NpmVersionManifest {
  return version as NpmVersionManifest
}
