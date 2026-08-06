import { compareVersion } from '../../../osv/versionCompare'

export type RubyGemsConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

type Op = '=' | '!=' | '>' | '<' | '>=' | '<='

interface RubyGemsClause {
  op: Op
  version: string
}

const OPERATORS: (Op | '~>')[] = ['!=', '>=', '<=', '~>', '>', '<', '=']

// deps.dev's d.Requirement for RubyGems is a Gem::Requirement string: comma-separated
// clauses ANDed together, each "<op> <version>" (bare version means "="). "~>" is the
// pessimistic operator — expands to a floor/ceiling pair below, not a single clause.
// Longest operators are checked first (a naive single regex backtracks ">=" into ">"
// plus a bogus "=" version when nothing follows the operator).
function parseClause(raw: string): RubyGemsClause[] | null {
  const trimmed = raw.trim()
  if (!trimmed) return null

  for (const op of OPERATORS) {
    if (trimmed.startsWith(op)) {
      const version = trimmed.slice(op.length).trim()
      if (!version) return null
      if (op === '~>') return expandPessimistic(version)
      return [{ op, version }]
    }
  }

  return [{ op: '=', version: trimmed }]
}

// "~> 1.2" means ">= 1.2, < 2.0"; "~> 1.2.3" means ">= 1.2.3, < 1.3.0" — drop the last
// segment and bump the segment now at the end, per Gem::Requirement's documented semantics.
function expandPessimistic(version: string): RubyGemsClause[] | null {
  const segments = version.split('.')
  if (segments.length < 2 || segments.some((s) => !/^\d+$/.test(s))) return null

  const bumped = segments.slice(0, -1)
  const last = Number(bumped[bumped.length - 1])
  bumped[bumped.length - 1] = String(last + 1)

  return [
    { op: '>=', version },
    { op: '<', version: bumped.join('.') },
  ]
}

function parseRequirement(constraint: string | null): RubyGemsClause[] | null {
  // package_dependencies.version_constraint is nullable (deps.dev fill path) — treat a
  // missing constraint the same as an unparseable one, not a crash on .split.
  if (constraint == null) return null
  const trimmed = constraint.trim()
  if (!trimmed) return null

  const clauses: RubyGemsClause[] = []
  for (const part of trimmed.split(',')) {
    const parsed = parseClause(part)
    if (!parsed) return null
    clauses.push(...parsed)
  }
  return clauses.length > 0 ? clauses : null
}

function clauseMatches(clause: RubyGemsClause, version: string): boolean {
  const c = compareVersion('rubygems', version, clause.version)
  if (c === null) return true // unparseable bound — over-inclusive

  switch (clause.op) {
    case '=':
      return c === 0
    case '!=':
      return c !== 0
    case '>':
      return c > 0
    case '<':
      return c < 0
    case '>=':
      return c >= 0
    case '<=':
      return c <= 0
  }
}

// Over-inclusive by design: the reachability stage (real source analysis) is the actual
// precision filter, so an unparseable constraint is always surfaced, never dropped.
//
// Checks against every vulnerable version, not just the highest one: a bounded "~>"
// constraint can include an older vulnerable version without including the max.
export function rubygemsConstraintMayInclude(
  constraint: string | null,
  vulnerableVersions: string[],
): RubyGemsConstraintMatch {
  const clauses = parseRequirement(constraint)
  if (!clauses) return 'unparseable-included'

  const matched = vulnerableVersions.some((version) =>
    clauses.every((clause) => clauseMatches(clause, version)),
  )
  return matched ? 'matched' : 'excluded'
}
