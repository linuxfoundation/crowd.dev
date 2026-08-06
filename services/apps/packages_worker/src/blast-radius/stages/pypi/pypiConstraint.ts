import { compareVersion } from '../../../osv/versionCompare'

export type PypiConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

type Op = '~=' | '===' | '==' | '!=' | '<=' | '>=' | '<' | '>'

interface PypiClause {
  op: Op
  version: string
  wildcard: boolean
}

// Longest operators first — a naive scan would let "==" swallow the first two chars of "===".
const OPERATORS: Op[] = ['~=', '===', '==', '!=', '<=', '>=', '<', '>']

// "~= V.N" expands to ">= V.N, == V.*" (drop the last release segment for the wildcard
// bound) per PEP 440's compatible-release operator; invalid with fewer than two segments.
function expandCompatible(version: string): PypiClause[] | null {
  const releaseMatch = version.match(/^[0-9]+(?:\.[0-9]+)*/)
  if (!releaseMatch) return null
  const segments = releaseMatch[0].split('.')
  if (segments.length < 2) return null

  const prefix = segments.slice(0, -1).join('.')
  return [
    { op: '>=', version, wildcard: false },
    { op: '==', version: prefix, wildcard: true },
  ]
}

// PEP 440 specifiers always carry an operator — unlike RubyGems there is no bare-version
// shorthand. Returns multiple clauses only for "~=", which expands to an AND pair.
function parseClause(raw: string): PypiClause[] | null {
  const trimmed = raw.trim()
  if (!trimmed) return null

  for (const op of OPERATORS) {
    if (!trimmed.startsWith(op)) continue

    let version = trimmed.slice(op.length).trim()
    if (!version) return null

    if (op === '~=') return expandCompatible(version)

    let wildcard = false
    if (version.endsWith('.*')) {
      if (op !== '==' && op !== '!=') return null
      wildcard = true
      version = version.slice(0, -2)
    }
    return [{ op, version, wildcard }]
  }

  return null
}

function parseSpecifierSet(constraint: string | null): PypiClause[] | null {
  // package_dependencies.version_constraint is nullable (deps.dev fill path) — treat a
  // missing constraint the same as an unparseable one, not a crash on .split.
  if (constraint == null) return null
  const trimmed = constraint.trim()
  if (!trimmed) return null

  const clauses: PypiClause[] = []
  for (const part of trimmed.split(',')) {
    const parsed = parseClause(part)
    if (!parsed) return null
    clauses.push(...parsed)
  }
  return clauses
}

// Wildcard matching reduces to a public-version string prefix check — deliberately
// over-inclusive rather than spec-exact, consistent with every other clause here.
function normalizedForWildcard(version: string): string {
  return version.trim().toLowerCase().replace(/^v/, '').split('+')[0]
}

function clauseMatches(clause: PypiClause, version: string): boolean {
  if (clause.op === '===') {
    return version.trim().toLowerCase() === clause.version.trim().toLowerCase()
  }

  if (clause.wildcard) {
    const matches = normalizedForWildcard(version).startsWith(clause.version.toLowerCase())
    return clause.op === '==' ? matches : !matches
  }

  const c = compareVersion('pypi', version, clause.version)
  if (c === null) return true // unparseable bound — over-inclusive

  switch (clause.op) {
    case '==':
      return c === 0
    case '!=':
      return c !== 0
    case '<':
      return c < 0
    case '>':
      return c > 0
    case '<=':
      return c <= 0
    case '>=':
      return c >= 0
    case '~=':
      return true // unreachable — expandCompatible never leaves '~=' on a leaf clause
  }
}

// Over-inclusive by design — reachability is the real precision filter. Checks every
// vulnerable version, not just the highest: a bounded "~=" can include an older one.
export function pypiConstraintMayInclude(
  constraint: string | null,
  vulnerableVersions: string[],
): PypiConstraintMatch {
  const clauses = parseSpecifierSet(constraint)
  if (!clauses) return 'unparseable-included'

  const matched = vulnerableVersions.some((version) =>
    clauses.every((clause) => clauseMatches(clause, version)),
  )
  return matched ? 'matched' : 'excluded'
}

// Prefers resolved version over the declared specifier (ground truth vs. declared) — PyPI
// is a deps.dev EDGE ecosystem, so a resolved version is usually available; mirrors
// cargoDependencyMayIncludeVuln.
export function pypiDependencyMayIncludeVuln(
  resolvedVersion: string | null,
  constraint: string | null,
  vulnerableVersions: string[],
): PypiConstraintMatch {
  if (resolvedVersion) {
    return vulnerableVersions.includes(resolvedVersion) ? 'matched' : 'excluded'
  }
  return pypiConstraintMayInclude(constraint, vulnerableVersions)
}
