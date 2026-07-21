// Advisory resolution (open | patched | null) for the akrites-external Advisories API.
//
// This is computed in TypeScript, NOT in SQL, on purpose: correct resolution needs
// real version-range membership, and the codebase already establishes that this must
// live in application code — see packages_worker/src/osv/deriveCriticalFlag.ts
// ("We compute the vulnerability decision in TypeScript because the comparator is
// ecosystem-specific ... and not expressible in SQL"). The internal
// getAdvisoriesByPackageId still uses the lexicographic SQL expression; only this
// external path was moved to correct TS logic for now.
//
// TODO(consolidate): packages_worker/src/osv/versionCompare.ts is the canonical
// ecosystem-aware comparator (npm semver + Maven), but it depends on `semver` and
// lives in an app, so it isn't importable here yet. This interim comparator handles
// clean dotted-numeric versions (the dominant OSV/npm shape) and returns null —
// i.e. "unknown", never a guessed open/patched — for anything it can't parse
// (prerelease/build metadata, non-numeric ecosystems). Unify on the shared
// comparator once it moves into a lib.

export interface AdvisoryAffectedRange {
  introduced: string | null
  fixed: string | null
  lastAffected: string | null
  rangeRaw: string | null
  unaffectedRaw: string | null
}

function parseNumeric(version: string): number[] | null {
  const trimmed = version.trim().replace(/^v/i, '')
  if (trimmed === '') return null
  const segments = trimmed.split('.')
  const out: number[] = []
  for (const seg of segments) {
    if (!/^\d+$/.test(seg)) return null
    out.push(parseInt(seg, 10))
  }
  return out
}

// -1 / 0 / 1 like Array#sort, or null when either operand isn't cleanly
// dotted-numeric (leading "v" tolerated). Null propagates to a null resolution
// so the API reports "unknown" instead of risking a wrong verdict.
function compareNumericVersion(a: string, b: string): number | null {
  const pa = parseNumeric(a)
  const pb = parseNumeric(b)
  if (!pa || !pb) return null
  const len = Math.max(pa.length, pb.length)
  for (let i = 0; i < len; i++) {
    const x = pa[i] ?? 0
    const y = pb[i] ?? 0
    if (x !== y) return x < y ? -1 : 1
  }
  return 0
}

// true = version is inside the affected range, false = outside, null = undeterminable.
// OSV semantics: introduced-inclusive, fixed-exclusive, last_affected-inclusive;
// introduced null/'0' means "from the beginning". Unlike deriveCriticalFlag (which
// treats an unparseable comparison as "no match" to under-flag), here an unparseable
// comparison yields null so resolution stays "unknown" rather than a false patched/open.
function isInRange(version: string, range: AdvisoryAffectedRange): boolean | null {
  if (range.introduced && range.introduced !== '0') {
    const c = compareNumericVersion(version, range.introduced)
    if (c === null) return null
    if (c < 0) return false
  }
  if (range.fixed) {
    const c = compareNumericVersion(version, range.fixed)
    if (c === null) return null
    if (c >= 0) return false
  }
  if (range.lastAffected) {
    const c = compareNumericVersion(version, range.lastAffected)
    if (c === null) return null
    if (c > 0) return false
  }
  return true
}

// deps.dev inserts raw-only rows (range_raw / unaffected_raw set, all structured
// bounds null). Those are unknowable, not "no fix" — excluding them prevents a
// raw-only row from dragging an otherwise-patched advisory to open. An OSV all-null
// MAL range (no raw payload) is NOT raw-only, so it's kept and resolves to open.
function isRawOnly(range: AdvisoryAffectedRange): boolean {
  return (
    range.introduced === null &&
    range.fixed === null &&
    range.lastAffected === null &&
    (range.rangeRaw !== null || range.unaffectedRaw !== null)
  )
}

export function resolveAdvisory(
  latestVersion: string | null,
  ranges: AdvisoryAffectedRange[],
): 'open' | 'patched' | null {
  if (!latestVersion) return null

  const structured = ranges.filter((r) => !isRawOnly(r))
  if (structured.length === 0) return null

  let anyUnknown = false
  for (const range of structured) {
    const inRange = isInRange(latestVersion, range)
    if (inRange === null) anyUnknown = true
    // In any affected range → still vulnerable, regardless of other undeterminable rows.
    else if (inRange) return 'open'
  }
  // Not inside any determinable range. If some rows were undeterminable we can't
  // claim it's patched — report unknown instead.
  return anyUnknown ? null : 'patched'
}
