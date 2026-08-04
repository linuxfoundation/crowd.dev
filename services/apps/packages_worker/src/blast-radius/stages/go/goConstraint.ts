import { compareVersion } from '../../../osv/versionCompare'

export type GoConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

// Go's `require` directive declares a floor (Minimal Version Selection), not a range
// like npm/semver — a dependent declaring `require example.com/mod v1.2.0` will build
// with v1.2.0 or anything MVS picks above it, never below. So a dependent is a
// candidate whenever its declared floor is <= the highest known vulnerable version;
// this is intentionally conservative (over-inclusive) since the reachability stage
// (stage 3, over real source) is the actual precision filter. An unparseable floor
// is never silently dropped — it's surfaced as a candidate for the same reason.
export function goConstraintMayInclude(
  constraint: string,
  maxVulnerableVersion: string,
): GoConstraintMatch {
  const comparison = compareVersion('go', constraint, maxVulnerableVersion)
  if (comparison === null) return 'unparseable-included'
  return comparison <= 0 ? 'matched' : 'excluded'
}
