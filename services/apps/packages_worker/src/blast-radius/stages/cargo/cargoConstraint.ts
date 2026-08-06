import * as semver from 'semver'

export type CargoConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

// Cargo bare versions ("1.2.3") mean caret, unlike node-semver's exact pin. Filters out
// unparseable requirements into the over-inclusive path (let reachability stage decide).
function toNodeSemverRange(cargoReq: string): string | null {
  const trimmed = cargoReq.trim()
  if (!trimmed) return null

  const translated = trimmed
    .split(',')
    .map((part) => {
      const clause = part.trim()
      if (!clause) return null
      if (/^[\^~=<>]/.test(clause) || /[xX*]/.test(clause)) return clause
      return `^${clause}`
    })
    .filter((x): x is string => x !== null)
    .join(' ')

  return translated || null
}

export function cargoConstraintMayInclude(
  constraint: string,
  vulnerableVersions: string[],
): CargoConstraintMatch {
  const range = constraint ? toNodeSemverRange(constraint) : null
  if (!range || !semver.validRange(range, { loose: true })) {
    return 'unparseable-included'
  }

  const matches = vulnerableVersions.some((v) => semver.satisfies(v, range, { loose: true }))
  return matches ? 'matched' : 'excluded'
}

// Prefers resolved version over requirement (ground truth vs. declared); mirrors
// goConstraint/mavenConstraint's resolved-version-first pattern.
export function cargoDependencyMayIncludeVuln(
  resolvedVersion: string | null,
  constraint: string,
  vulnerableVersions: string[],
): CargoConstraintMatch {
  if (resolvedVersion) {
    return vulnerableVersions.includes(resolvedVersion) ? 'matched' : 'excluded'
  }
  return cargoConstraintMayInclude(constraint, vulnerableVersions)
}
