import * as semver from 'semver'

export type CargoConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

// Cargo's requirement grammar (https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html)
// is close to but not identical to node-semver's: a bare version ("1.2.3") means caret
// ("^1.2.3", compatible updates), not an exact pin like node-semver treats it. `^ ~ = *`
// operators and comparator ranges (">=1.2, <1.5") already mean the same thing in both
// grammars, so only the bare/partial-version default needs translating. An unparseable
// requirement is never silently dropped — same over-inclusive rationale as goConstraint.ts:
// the reachability stage (stage 3, over real source) is the actual precision filter.
function toNodeSemverRange(cargoReq: string): string | null {
  const trimmed = cargoReq.trim()
  if (!trimmed) return null

  const translated = trimmed
    .split(',')
    .map((part) => {
      const clause = part.trim()
      if (!clause) return null
      // Already an explicit operator or wildcard — pass through as-is. Cargo has no
      // hyphen-range syntax, so a bare `-` here is always a prerelease tag (e.g.
      // "1.2.3-alpha"), not a range operator, and must still get the caret prefix.
      if (/^[\^~=<>]/.test(clause) || /[xX*]/.test(clause)) return clause
      // Bare version ("1.2.3", "1.2", "1") defaults to caret per Cargo semantics.
      return `^${clause}`
    })
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

  const matches = vulnerableVersions.some((v) =>
    semver.satisfies(v, range, { loose: true, includePrerelease: true }),
  )
  return matches ? 'matched' : 'excluded'
}

// Prefers the concrete resolved version (the edge's actual installed version) over the
// declared requirement when available — mirrors goConstraint/mavenConstraint's
// resolved-version-first pattern, since a resolved version is ground truth while the
// requirement is only what the dependent's Cargo.toml declares.
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
