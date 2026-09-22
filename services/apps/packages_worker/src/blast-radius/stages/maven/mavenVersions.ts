import { OsvAffectedPackage } from '../../clients/osvClient'
import * as ecosystemVersions from '../ecosystemVersions'

// Maven-specific counterpart to semverRange.ts — OSV Maven advisories use ECOSYSTEM-typed
// ranges, ordered via compareVersion('maven', …) instead of node-semver. Thin wrappers
// binding ecosystem='maven' over the shared ecosystemVersions.ts logic (also used by NuGet).
export type MavenRange = ecosystemVersions.EcosystemRange

export function mavenRangeEvents(entry: OsvAffectedPackage): MavenRange[] {
  return ecosystemVersions.ecosystemRangeEvents(entry)
}

export function versionsInRanges(versions: string[], ranges: MavenRange[]): string[] {
  return ecosystemVersions.versionsInRanges('maven', versions, ranges)
}

export function highestVersion(versions: string[]): string | null {
  return ecosystemVersions.highestVersion('maven', versions)
}
