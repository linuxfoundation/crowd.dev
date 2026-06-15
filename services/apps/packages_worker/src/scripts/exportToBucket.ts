import {
  OsspckgsJobKind,
  createIngestJob,
  findExportByKindAndName,
  markJobStatus,
} from '@crowd/data-access-layer'

import { getPackagesDb } from '../db'
import { extractBqStats } from '../deps-dev/bqStats'
import { GCS_BUCKET, bigquery, bucket } from '../deps-dev/config'
import { ADVISORIES_SQL, buildAdvisoryPackagesSql } from '../deps-dev/queries/advisoriesSql'
import { buildDependentCountsSql } from '../deps-dev/queries/dependentCountsSql'
import { buildDepsFullSql } from '../deps-dev/queries/depsSql'
import { buildPackageReposSql } from '../deps-dev/queries/packageReposSql'
import { buildPackagesFullSql } from '../deps-dev/queries/packagesSql'
import { buildReposSql } from '../deps-dev/queries/reposSql'
import { toSystemsFilter } from '../deps-dev/queries/systems'
import { buildVersionsFullSql } from '../deps-dev/queries/versionsSql'
import { SCORECARD_CHECKS_SQL, SCORECARD_REPOS_SQL } from '../scorecard/queries/scorecardSql'

const HELP = `
Usage: export-to-bucket <export-name> <parts> [options]

Pre-exports BQ data to GCS under a named label. Run before bootstrap to skip
BQ billing during ingestion. Safe to re-run — already-exported parts are skipped.

Arguments:
  export-name    Unique label for this export group (e.g. cargo-may-2026).
                 Used as GCS folder name and DB lookup key.
  parts          Comma-separated list of parts to export, or "all":
                   packages          PackageVersionsLatest
                   versions          PackageVersionsLatest
                   deps              DependencyGraphEdgesLatest (or DependenciesLatest with --deps-table-b)
                   repos             PackageVersionToProject + ProjectsLatest (snapshot date auto-resolved)
                   package_repos     PackageVersionToProject + PackageVersionsLatest purl map (same snapshot)
                   counts            Dependents (snapshot date auto-resolved)
                   advisories        AdvisoriesLatest (no ecosystem filter)
                   advisory_packages AdvisoriesLatest + PackageVersionsLatest
                   scorecard_repos   OpenSSF Scorecard aggregate scores (scorecard-v2_latest)
                   scorecard_checks  OpenSSF Scorecard per-check detail (scorecard-v2_latest UNNEST)

Options:
  --ecosystems A,B        Comma-separated ecosystems: CARGO,NPM,MAVEN,GO,PYPI,NUGET
                          Default: all 6
  --snapshot-date DATE    Partition date (YYYY-MM-DD) for repos, package_repos, and counts.
                          Auto-resolved from BQ if omitted.
  --deps-table-b          Use DependenciesLatest for deps (cheaper, no version_constraint).
                          ADR-0003 Option B. ~$4.69 vs $12.67 for CARGO.
  --dry-run               Print BQ size and cost estimates only. No exports run.
  --help                  Show this help

Resume behaviour:
  Re-run with the same export-name. Parts with DB record + GCS files present are
  skipped. If DB was wiped (scaffold reset) but GCS files exist, they are
  re-registered in DB without hitting BQ.

Examples:
  pnpm export-to-bucket:local cargo-may-2026 all --ecosystems CARGO --dry-run
  pnpm export-to-bucket:local cargo-may-2026 all --ecosystems CARGO
  pnpm export-to-bucket:local cargo-may-2026 all --ecosystems CARGO --deps-table-b
  pnpm export-to-bucket:local cargo-may-2026 packages,versions,deps --ecosystems CARGO
  pnpm export-to-bucket:local cargo-may-2026 counts --snapshot-date 2026-05-25

After exporting, trigger bootstrap with:
  pnpm trigger-bootstrap:local full CARGO --export-name cargo-may-2026
`

type ExportPart =
  | 'packages'
  | 'versions'
  | 'deps'
  | 'repos'
  | 'package_repos'
  | 'counts'
  | 'advisories'
  | 'advisory_packages'
  | 'scorecard_repos'
  | 'scorecard_checks'

const PART_TO_KIND: Record<ExportPart, OsspckgsJobKind> = {
  packages: 'packages',
  versions: 'versions',
  deps: 'package_dependencies',
  repos: 'repos',
  package_repos: 'package_repos',
  counts: 'dependent_counts',
  advisories: 'advisories',
  advisory_packages: 'advisory_packages',
  scorecard_repos: 'scorecard_repos',
  scorecard_checks: 'scorecard_checks',
}

const ALL_PARTS: ExportPart[] = [
  'packages',
  'versions',
  'deps',
  'repos',
  'package_repos',
  'counts',
  'advisories',
  'advisory_packages',
  'scorecard_repos',
  'scorecard_checks',
]

async function resolveSnapshotDate(table: string, today: string): Promise<string> {
  const query = `
    SELECT MAX(SnapshotAt) AS snapshot_date
    FROM \`${table}\`
    WHERE SnapshotAt >= TIMESTAMP(DATE_SUB(DATE '${today}', INTERVAL 30 DAY))
      AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
  `
  const [job] = await bigquery.createQueryJob({ query, location: 'US' })
  const [rows] = await job.getQueryResults()
  const raw = rows[0]?.snapshot_date
  if (!raw) throw new Error(`No snapshot found within 30 days of ${today} for ${table}`)
  const val = typeof raw === 'string' ? raw : (raw?.value ?? String(raw))
  return val.slice(0, 10)
}

async function exportPart(opts: {
  exportName: string
  part: ExportPart
  sql: string
  dryRunOnly: boolean
}): Promise<void> {
  const { exportName, part, sql, dryRunOnly } = opts
  const jobKind = PART_TO_KIND[part]
  const gcsPrefix = `gs://${GCS_BUCKET}/osspckgs/${jobKind}/exports/${exportName}/`
  const gcsFolderPath = `osspckgs/${jobKind}/exports/${exportName}/`

  const qx = await getPackagesDb()

  // Resume check: skip if already exported and GCS files still exist
  const prior = await findExportByKindAndName(qx, jobKind, exportName)
  if (prior) {
    const priorPath = prior.gcsPrefix.replace(`gs://${GCS_BUCKET}/`, '')
    const [priorFiles] = await bucket.getFiles({ prefix: priorPath, maxResults: 1 })
    if (priorFiles.length > 0) {
      console.log(
        `  ✓ ${part} (${jobKind}) — already exported (job #${prior.id}, ${prior.rowCountBq.toLocaleString()} rows, GCS files present), skipping`,
      )
      return
    }
    console.log(
      `  ⚠ ${part} (${jobKind}) — found in DB (job #${prior.id}) but GCS files are gone, re-exporting`,
    )
  }

  // GCS-first recovery: if files exist in the bucket but DB has no record (e.g. after scaffold reset),
  // register a DB record from the existing GCS data instead of re-running BQ.
  const [existingFiles] = await bucket.getFiles({ prefix: gcsFolderPath, maxResults: 1 })
  if (existingFiles.length > 0) {
    if (dryRunOnly) {
      console.log(
        `  ✓ ${part} (${jobKind}) — GCS files exist (no DB record, would register on real run), skipping BQ`,
      )
      return
    }
    console.log(
      `  ↻ ${part} (${jobKind}) — GCS files exist but no DB record (scaffold reset?), registering...`,
    )
    const jobId = await createIngestJob(qx, jobKind, 'full', null, exportName)
    await markJobStatus(qx, jobId, 'exported', {
      gcsPrefix,
      rowCountBq: 0,
      bqBytesBilled: 0,
      tableRowCounts: { 'bq:export': 0 },
    })
    console.log(`    ✓ registered as job #${jobId} (row count unknown after reset)`)
    return
  }

  // Dry-run to estimate cost
  const [dryRunJob] = await bigquery.createQueryJob({ query: sql, dryRun: true, location: 'US' })
  const dryRunBytes = Number(dryRunJob.metadata.statistics.totalBytesProcessed ?? 0)
  const costUsd = (dryRunBytes / 1e12) * 5
  console.log(
    `  ${dryRunOnly ? '~' : '→'} ${part} (${jobKind}) — ${(dryRunBytes / 1e9).toFixed(1)} GB, ~$${costUsd.toFixed(4)}`,
  )

  if (dryRunOnly) return

  const jobId = await createIngestJob(qx, jobKind, 'full', null, exportName)
  await markJobStatus(qx, jobId, 'exporting')

  const exportSql = `
CREATE TEMP TABLE _export_data AS SELECT * FROM (${sql.replace(/;\s*$/, '')});
EXPORT DATA OPTIONS(
  uri='${gcsPrefix}*.parquet',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
) AS SELECT * FROM _export_data;
`

  const start = Date.now()
  const [job] = await bigquery.createQueryJob({ query: exportSql, location: 'US' })
  await job.promise()
  const stats = await extractBqStats(job, bigquery)
  const durationSec = ((Date.now() - start) / 1000).toFixed(0)

  await markJobStatus(qx, jobId, 'exported', {
    gcsPrefix,
    rowCountBq: stats.outputRows ?? 0,
    bqBytesBilled: stats.bqBytesBilled,
    bqJobId: stats.bqJobId,
    bqStats: stats,
    tableRowCounts: { 'bq:export': stats.outputRows ?? 0 },
  })

  const actualCost = (stats.bqBytesBilled / 1e12) * 5
  console.log(
    `    ✓ done — ${(stats.outputRows ?? 0).toLocaleString()} rows, $${actualCost.toFixed(4)} billed, ${durationSec}s (job #${jobId})`,
  )
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h')) {
    console.log(HELP)
    process.exit(0)
  }

  const exportName = args[0]
  if (!exportName || exportName.startsWith('--')) {
    console.error('Error: missing export-name argument\n')
    console.error(HELP)
    process.exit(1)
  }

  const partsArg = args[1]
  if (!partsArg || partsArg.startsWith('--')) {
    console.error('Error: missing parts argument\n')
    console.error(HELP)
    process.exit(1)
  }

  const requestedParts: ExportPart[] =
    partsArg === 'all' ? ALL_PARTS : (partsArg.split(',').map((p) => p.trim()) as ExportPart[])

  const invalidParts = requestedParts.filter((p) => !ALL_PARTS.includes(p))
  if (invalidParts.length > 0) {
    console.error(`Unknown parts: ${invalidParts.join(', ')}`)
    console.error(`Valid parts: ${ALL_PARTS.join(', ')}`)
    process.exit(1)
  }

  const ecosystemsIdx = args.indexOf('--ecosystems')
  const ecosystems =
    ecosystemsIdx !== -1
      ? args[ecosystemsIdx + 1]?.split(',').map((e) => e.trim().toUpperCase())
      : undefined

  const snapshotDateArgIdx = args.indexOf('--snapshot-date')
  const snapshotDate = snapshotDateArgIdx !== -1 ? args[snapshotDateArgIdx + 1] : undefined

  const depsTableOption: 'A' | 'B' = args.includes('--deps-table-b') ? 'B' : 'A'
  const dryRunOnly = args.includes('--dry-run')

  const today = new Date().toISOString().slice(0, 10)
  const systems = toSystemsFilter(ecosystems)

  const needsReposSnapshot = requestedParts.some((p) => p === 'repos' || p === 'package_repos')
  const needsCountsSnapshot = requestedParts.includes('counts')

  // Resolve snapshot dates independently — PackageVersionToProject and Dependents may differ.
  let reposSnapshotDate = snapshotDate
  let countsSnapshotDate = snapshotDate

  if (needsReposSnapshot && !reposSnapshotDate) {
    process.stdout.write(
      '  Resolving snapshot date for repos/package_repos (PackageVersionToProject)... ',
    )
    reposSnapshotDate = await resolveSnapshotDate(
      'bigquery-public-data.deps_dev_v1.PackageVersionToProject',
      today,
    )
    console.log(reposSnapshotDate)
  }

  if (needsCountsSnapshot && !countsSnapshotDate) {
    process.stdout.write('  Resolving snapshot date for counts (Dependents)... ')
    countsSnapshotDate = await resolveSnapshotDate(
      'bigquery-public-data.deps_dev_v1.Dependents',
      today,
    )
    console.log(countsSnapshotDate)
  }

  const SQL_BY_PART: Record<ExportPart, string> = {
    packages: buildPackagesFullSql(systems),
    versions: buildVersionsFullSql(systems),
    deps: buildDepsFullSql(systems, depsTableOption),
    repos: buildReposSql(reposSnapshotDate ?? today, systems),
    package_repos: buildPackageReposSql(reposSnapshotDate ?? today, systems),
    counts: buildDependentCountsSql(countsSnapshotDate ?? today),
    advisories: ADVISORIES_SQL,
    advisory_packages: buildAdvisoryPackagesSql(systems),
    scorecard_repos: SCORECARD_REPOS_SQL,
    scorecard_checks: SCORECARD_CHECKS_SQL,
  }

  const ecosystemLabel = ecosystems ? ecosystems.join(',') : 'all'
  const tableLabel = depsTableOption === 'B' ? ' [deps=Option B]' : ''
  console.log(
    `\nexport-name: ${exportName}  ecosystems: ${ecosystemLabel}${tableLabel}  mode: ${dryRunOnly ? 'dry-run' : 'export'}\n`,
  )

  for (const part of requestedParts) {
    await exportPart({
      exportName,
      part,
      sql: SQL_BY_PART[part],
      dryRunOnly,
    })
  }

  if (!dryRunOnly) {
    console.log(`\nDone. Trigger bootstrap with:`)
    const ecoFlag = ecosystems ? ` ${ecosystems.join(',')}` : ''
    console.log(`  pnpm trigger-bootstrap        full${ecoFlag} --export-name ${exportName}`)
    console.log(`  pnpm trigger-bootstrap:local  full${ecoFlag} --export-name ${exportName}`)
  }
}

main().catch((err) => {
  console.error('Export failed:', err)
  process.exit(1)
})
