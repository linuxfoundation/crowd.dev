import { OsspckgsJobKind } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { bigquery } from '../config'

const log = getServiceChildLogger('resolveSnapshotDate')

// Maps each partitioned job kind to its BQ table; advisory kinds use AdvisoriesLatest (no snapshots).
// repos/package_repos/dependent_counts query PackageVersionToProject or Dependents — both on weekly
// cadence that differs from PackageVersions (which may publish more frequently).
const TABLE_BY_KIND: Partial<Record<OsspckgsJobKind, string>> = {
  packages: 'bigquery-public-data.deps_dev_v1.PackageVersions',
  versions: 'bigquery-public-data.deps_dev_v1.PackageVersions',
  package_dependencies: 'bigquery-public-data.deps_dev_v1.DependencyGraphEdges',
  repos: 'bigquery-public-data.deps_dev_v1.PackageVersionToProject',
  package_repos: 'bigquery-public-data.deps_dev_v1.PackageVersionToProject',
  dependent_counts: 'bigquery-public-data.deps_dev_v1.Dependents',
}

export interface ResolveSnapshotDateInput {
  jobKind: OsspckgsJobKind
  today: string // YYYY-MM-DD upper bound
}

export interface ResolveSnapshotDateOutput {
  snapshotDate: string // YYYY-MM-DD of the latest published snapshot <= today
}

// deps.dev publishes weekly snapshots; querying WHERE SnapshotAt = today misses 6/7 days.
// This activity finds the most recent available partition date, scanning only partition metadata.
export async function resolveSnapshotDate(
  input: ResolveSnapshotDateInput,
): Promise<ResolveSnapshotDateOutput> {
  const table = TABLE_BY_KIND[input.jobKind]
  if (!table) {
    throw new Error(`resolveSnapshotDate called for non-partitioned kind: ${input.jobKind}`)
  }

  const query = `
    SELECT MAX(SnapshotAt) AS snapshot_date
    FROM \`${table}\`
    WHERE SnapshotAt >= TIMESTAMP(DATE_SUB(DATE '${input.today}', INTERVAL 30 DAY))
      AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${input.today}', INTERVAL 1 DAY))
  `

  const [job] = await bigquery.createQueryJob({ query, location: 'US' })
  const [rows] = await job.getQueryResults()

  const raw = rows[0]?.snapshot_date
  if (!raw) {
    throw new Error(`No deps.dev snapshot found within 30 days of ${input.today} for ${table}`)
  }

  // BQ returns TIMESTAMP as {value: '2026-05-20 00:00:00 UTC'} or similar
  const snapshotDate =
    typeof raw === 'string'
      ? raw.slice(0, 10)
      : (raw?.value?.slice(0, 10) ?? String(raw).slice(0, 10))

  log.info({ jobKind: input.jobKind, snapshotDate }, 'Resolved snapshot date')
  return { snapshotDate }
}
