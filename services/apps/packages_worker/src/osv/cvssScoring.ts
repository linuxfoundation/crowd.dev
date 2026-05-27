// CVSS v3.1 base score, computed from a CVSS:3.x/... vector string per the FIRST
// specification at https://www.first.org/cvss/v3.1/specification-document.
//
// CVSS v4.0 is not implemented here. v4 scoring requires the macro-vector lookup
// table from the FIRST v4.0 spec (~270 entries). For v1 we treat V4-only records
// as missing a numeric vector and let extractSeverity fall back to the qualitative
// tag; in practice OSV records that include V4 almost always also include V3 (per
// the 2026-05-27 spike). A proper v4 scorer is tracked as a follow-up.

const AV: Record<string, number> = { N: 0.85, A: 0.62, L: 0.55, P: 0.2 }
const AC: Record<string, number> = { L: 0.77, H: 0.44 }
const PR_U: Record<string, number> = { N: 0.85, L: 0.62, H: 0.27 }
const PR_C: Record<string, number> = { N: 0.85, L: 0.68, H: 0.5 }
const UI: Record<string, number> = { N: 0.85, R: 0.62 }
const CIA: Record<string, number> = { N: 0, L: 0.22, H: 0.56 }
const S_VALUES = new Set(['U', 'C'])

function parseVector(vector: string): Record<string, string> | null {
  const parts = vector.split('/').filter(Boolean)
  if (parts.length === 0) return null
  const out: Record<string, string> = {}
  for (const part of parts) {
    const eq = part.indexOf(':')
    if (eq < 0) return null
    out[part.slice(0, eq)] = part.slice(eq + 1)
  }
  return out
}

// CVSS spec roundUp1: ceil to one decimal place, with tolerance for FP drift.
function roundUp(x: number): number {
  const scaled = Math.round(x * 100000)
  if (scaled % 10000 === 0) return scaled / 100000
  return (Math.floor(scaled / 10000) + 1) / 10
}

export function computeV3Score(vector: string): number | null {
  const v = parseVector(vector)
  if (!v) return null
  if (!v.CVSS || !v.CVSS.startsWith('3.')) return null

  const av = AV[v.AV]
  const ac = AC[v.AC]
  const ui = UI[v.UI]
  // Scope is read directly because the metric is qualitative, not numeric, so
  // it does not slot into the undefined-check below. Validate it explicitly
  // here — an invalid or missing S would otherwise silently fall through to
  // the Scope:Unchanged formula and produce a wrong numeric score instead of
  // null, which is the headline risk flagged in ADR-0005.
  const s = v.S
  if (!S_VALUES.has(s)) return null
  const pr = s === 'C' ? PR_C[v.PR] : PR_U[v.PR]
  const c = CIA[v.C]
  const i = CIA[v.I]
  const a = CIA[v.A]
  if ([av, ac, ui, pr, c, i, a].some((x) => x === undefined)) return null

  const iss = 1 - (1 - c) * (1 - i) * (1 - a)
  const impact = s === 'C' ? 7.52 * (iss - 0.029) - 3.25 * Math.pow(iss - 0.02, 15) : 6.42 * iss

  if (impact <= 0) return 0

  const exploitability = 8.22 * av * ac * pr * ui
  const raw = s === 'C' ? 1.08 * (impact + exploitability) : impact + exploitability
  return roundUp(Math.min(raw, 10))
}
