import { ApplicationFailure } from '@temporalio/client'
import { OsspckgsJobKind } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { bigquery } from '../config'

const log = getServiceChildLogger('probePartitionExists')

const TABLE_BY_KIND: Partial<Record<OsspckgsJobKind, string>> = {
  packages: 'bigquery-public-data.deps_dev_v1.PackageVersions',
  versions: 'bigquery-public-data.deps_dev_v1.PackageVersions',
  package_dependencies: 'bigquery-public-data.deps_dev_v1.DependencyGraphEdges',
}

export interface ProbePartitionExistsInput {
  jobKind: OsspckgsJobKind
  snapshotAt: string // YYYY-MM-DD
}

// Verifies that the resolved snapshotAt partition actually has rows in BQ.
// Runs after resolveSnapshotDate so the date is known-good, but this is cheap insurance
// against a partition that exists in metadata but has no data (shouldn't happen, but guards
// the downstream incremental diff from silently reading zero rows).
export async function probePartitionExists(input: ProbePartitionExistsInput): Promise<void> {
  const table = TABLE_BY_KIND[input.jobKind]
  if (!table) {
    // advisory kinds use *Latest views — no partition probe needed
    log.info({ jobKind: input.jobKind }, 'partition probe skipped (non-partitioned kind)')
    return
  }

  const query = `
    SELECT 1
    FROM \`${table}\`
    WHERE SnapshotAt >= TIMESTAMP('${input.snapshotAt}')
      AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${input.snapshotAt}', INTERVAL 1 DAY))
    LIMIT 1
  `

  const [job] = await bigquery.createQueryJob({ query, location: 'US' })
  const [rows] = await job.getQueryResults()

  if (rows.length === 0) {
    throw ApplicationFailure.nonRetryable(
      `No rows found in ${table} for SnapshotAt = ${input.snapshotAt} — partition missing or empty`,
    )
  }

  log.info({ jobKind: input.jobKind, snapshotAt: input.snapshotAt }, 'Partition probe passed')
}
