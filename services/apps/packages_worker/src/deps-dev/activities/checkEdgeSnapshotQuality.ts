import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { bigquery } from '../config'
import { assertSnapshotDate } from '../queries/depsSql'

// deps.dev ships weekly full snapshots of the resolved dependency graph in
// DependencyGraphEdges (NPM/MAVEN/PYPI/CARGO). On 2026-06-11 and 2026-06-15 that
// pipeline shipped corrupt: every (package, version) collapsed to 1–2 arbitrary edges,
// each duplicated exactly ~100×. An incremental package_dependencies run against a
// corrupt snapshot exports ~550M garbage rows and burns ~5h before PG's
// ON CONFLICT DO NOTHING discards 99.5% of them — wasteful, though not data-destroying.
//
// This guard runs BEFORE the BQ export. It probes a small set of high-fanout canary packages
// over the today partition and rejects the snapshot when the duplication ratio
// (rows / distinct edges) is far above the healthy baseline of ~1.0 (corrupt ≈ 100).
//
// Cost: the filter is written as `(System = x AND Name IN (...))` OR-groups so it prunes on the
// table's clustering (System, Name, Version) — measured ~3.8GB billed (≈ $0.02), vs ~1.76TB if
// the predicate is a `(System, Name) IN UNNEST(structs)` tuple (which defeats cluster pruning).
//
// GO/NUGET are sourced from manifest tables (GoRequirements/NuGetRequirements) which are not
// produced by the resolution pipeline and were unaffected, so they are not probed here.

export interface CheckEdgeSnapshotQualityInput {
  snapshotDate: string // YYYY-MM-DD — the resolved BQ partition date being ingested
  ecosystems: string[]
}

export interface CanaryStat {
  system: string
  name: string
  rows: number
  edges: number
  ratio: number | null // rows / distinct edges; ~1.0 healthy, ~100 when corrupt
  present: boolean
}

export interface CheckEdgeSnapshotQualityOutput {
  ok: boolean
  reason?: string
  canaries: CanaryStat[]
}

// Systems whose direct deps come from the resolved graph (DependencyGraphEdges).
// Mirrors EDGE_SYSTEMS in queries/depsSql.ts — GO/NUGET are manifest-sourced and excluded.
const EDGE_SYSTEMS = new Set(['NPM', 'MAVEN', 'PYPI', 'CARGO'])

// High-fanout packages that every healthy snapshot resolves with MANY distinct direct edges.
// All verified present with ratio ≈ 1.0 on the healthy 2026-06-01 snapshot. Deliberately avoid
// dependency-free packages (e.g. lodash, urllib3) — they have zero direct edges even when healthy,
// so they'd masquerade as "missing". Keep several per system so a single yank can't trip the guard.
const CANARIES: ReadonlyArray<{ system: string; name: string }> = [
  { system: 'NPM', name: 'express' },
  { system: 'NPM', name: 'webpack' },
  { system: 'NPM', name: 'react' },
  { system: 'NPM', name: 'eslint' },
  { system: 'NPM', name: '@babel/core' },
  { system: 'MAVEN', name: 'com.google.guava:guava' },
  { system: 'MAVEN', name: 'org.springframework:spring-context' },
  { system: 'MAVEN', name: 'org.apache.httpcomponents:httpclient' },
  { system: 'PYPI', name: 'requests' },
  { system: 'PYPI', name: 'flask' },
  { system: 'PYPI', name: 'django' },
  { system: 'PYPI', name: 'pandas' },
  { system: 'CARGO', name: 'serde' },
  { system: 'CARGO', name: 'tokio' },
  { system: 'CARGO', name: 'clap' },
  { system: 'CARGO', name: 'reqwest' },
]

// Healthy ratio is ~1.0 (max observed ≈ 1.8 for serde); the corrupt snapshots ran ~100×.
// 5 leaves wide margin for benign multiplicity while catching the ×100 collapse unambiguously.
const DUP_RATIO_REJECT = 5

interface CanaryRow {
  system: string
  name: string
  row_count: number | string | { value: string }
  edge_count: number | string | { value: string }
}

// BigQuery returns INT64 columns as number | string | { value }; coerce defensively.
function toNum(v: number | string | { value: string }): number {
  if (typeof v === 'number') return v
  if (typeof v === 'string') return Number(v)
  return Number(v.value)
}

export async function checkEdgeSnapshotQuality(
  input: CheckEdgeSnapshotQualityInput,
): Promise<CheckEdgeSnapshotQualityOutput> {
  const inScope = new Set(
    input.ecosystems.map((e) => e.toUpperCase()).filter((e) => EDGE_SYSTEMS.has(e)),
  )
  const activeCanaries = CANARIES.filter((c) => inScope.has(c.system))

  // No resolved-graph ecosystems requested (e.g. GO/NUGET only) → nothing to probe.
  if (activeCanaries.length === 0) {
    return { ok: true, canaries: [] }
  }

  assertSnapshotDate(input.snapshotDate)

  // Group canaries by system into `(System = x AND Name IN (...))` predicates so the filter prunes
  // on the (System, Name, Version) clustering — keeps the probe in the single-GB / pennies range.
  const bySystem = new Map<string, string[]>()
  for (const c of activeCanaries) {
    const names = bySystem.get(c.system) ?? []
    names.push(c.name)
    bySystem.set(c.system, names)
  }
  const systemPredicates = [...bySystem.entries()]
    .map(
      ([system, names]) =>
        `(e.System = '${system}' AND e.Name IN (${names.map((n) => `'${n}'`).join(', ')}))`,
    )
    .join('\n        OR ')

  // Direct edges only (From = graph root), matching what the ingest reads. %T formats NULLs
  // as the literal "NULL" so COUNT(DISTINCT ...) isn't nulled out by unresolved To.Version.
  const query = `
    SELECT
      e.System AS system,
      e.Name   AS name,
      COUNT(*) AS row_count,
      COUNT(DISTINCT FORMAT('%T|%T|%T', e.From.Version, e.To.Name, e.To.Version)) AS edge_count
    FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdges\` e
    WHERE e.SnapshotAt >= TIMESTAMP('${input.snapshotDate}')
      AND e.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${input.snapshotDate}', INTERVAL 1 DAY))
      AND e.From.Name = e.Name AND e.From.Version = e.Version
      AND (
        ${systemPredicates}
      )
    GROUP BY e.System, e.Name
  `

  const [job] = await bigquery.createQueryJob({ query, location: 'US' })
  const [rows] = await job.getQueryResults()
  const resultRows = rows as CanaryRow[]

  const canaries: CanaryStat[] = activeCanaries.map((c) => {
    const r = resultRows.find((row) => row.system === c.system && row.name === c.name)
    if (!r) {
      return { system: c.system, name: c.name, rows: 0, edges: 0, ratio: null, present: false }
    }
    const rowCount = toNum(r.row_count)
    const edgeCount = toNum(r.edge_count)
    return {
      system: c.system,
      name: c.name,
      rows: rowCount,
      edges: edgeCount,
      ratio: edgeCount > 0 ? rowCount / edgeCount : null,
      present: true,
    }
  })

  // Corruption shows up as a ×100 duplication ratio on the canaries that DO resolve, and/or as
  // canaries vanishing entirely. Base the verdict on the ratio of PRESENT canaries (missingness
  // alone is noisy — packages get yanked), with a floor against a mass collapse.
  const present = canaries.filter((c) => c.present)
  const overDuplicated = present.filter((c) => c.ratio !== null && c.ratio > DUP_RATIO_REJECT)
  const minPresent = Math.ceil(activeCanaries.length / 2)
  const massCollapse = present.length < minPresent
  const majorityOverDuplicated = overDuplicated.length >= Math.ceil(present.length / 2)
  const ok = !massCollapse && !majorityOverDuplicated

  if (ok) {
    return { ok: true, canaries }
  }

  const dupDetail = overDuplicated
    .map((c) => `${c.system}:${c.name} ratio=${(c.ratio ?? 0).toFixed(1)} (${c.rows}/${c.edges})`)
    .join(', ')
  const reason = massCollapse
    ? `only ${present.length}/${activeCanaries.length} canaries resolved (min ${minPresent}) — snapshot looks collapsed`
    : `${overDuplicated.length}/${present.length} present canaries over-duplicated (ratio > ${DUP_RATIO_REJECT}; healthy ≈ 1.0): ${dupDetail}`

  sendSlackNotification(
    SlackChannel.CDP_CRITICAL_ALERTS,
    SlackPersona.CRITICAL_ALERTER,
    ':warning: deps.dev edge snapshot quality anomaly detected',
    [
      {
        title: 'Snapshot',
        text: input.snapshotDate,
      },
      {
        title: 'Canaries',
        text: reason,
      },
      {
        title: 'Action',
        text: 'package_dependencies ingest aborted before export — existing rows preserved. The deps.dev resolved-graph snapshot looks corrupt; re-run once a healthy snapshot is published.',
      },
    ],
  )

  return { ok: false, reason, canaries }
}
