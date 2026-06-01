import { createIngestJob, markJobStatus } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { extractBqStats } from '../bqStats'
import { bigquery } from '../config'
import { buildDependentCountsSql } from '../queries/dependentCountsSql'
import { formatValue } from '../sqlUtils'

const log = getServiceChildLogger('updateDependentCounts')

const BATCH_SIZE = 5000

interface DependentRow {
  purl: string
  dependent_packages_count: bigint | number
  dependent_repos_count: bigint | number
}

export interface UpdateDependentCountsInput {
  runId: string
  snapshotDate: string // YYYY-MM-DD — must be an available Dependents partition date
}

export interface UpdateDependentCountsOutput {
  rowsUpdated: number
}

export async function updateDependentCounts(
  input: UpdateDependentCountsInput,
): Promise<UpdateDependentCountsOutput> {
  const qx = await getPackagesDb()
  const jobId = await createIngestJob(qx, 'dependent_counts', 'full', new Date(input.snapshotDate))
  await markJobStatus(qx, jobId, 'loading')

  const [job] = await bigquery.createQueryJob({ query: buildDependentCountsSql(input.snapshotDate), location: 'US' })
  await job.promise()
  const bqStats = await extractBqStats(job)
  const stream = job.getQueryResultsStream()

  let batch: DependentRow[] = []
  let totalUpdated = 0
  let totalFromBq = 0

  const flush = async () => {
    if (batch.length === 0) return
    const valuesClause = batch
      .map(
        (r) =>
          `(${formatValue(r.purl)}, ${formatValue(r.dependent_packages_count)}, ${formatValue(r.dependent_repos_count)})`,
      )
      .join(',\n')
    const sql = `
      UPDATE packages SET
        dependent_packages_count = data.pkg_count::bigint,
        dependent_repos_count    = data.repo_count::bigint,
        last_synced_at           = NOW()
      FROM (VALUES ${valuesClause}) AS data(purl, pkg_count, repo_count)
      WHERE packages.purl = data.purl
    `
    const updated = await qx.result(sql)
    totalUpdated += updated
    batch = []
  }

  for await (const row of stream) {
    totalFromBq++
    batch.push(row as DependentRow)
    if (batch.length >= BATCH_SIZE) {
      await flush()
    }
  }
  await flush()

  await markJobStatus(qx, jobId, 'done', {
    finishedAt: new Date(),
    rowCountBq: totalFromBq,
    rowCountPg: totalUpdated,
    bqJobId: bqStats.bqJobId,
    bqBytesBilled: bqStats.bqBytesBilled,
    bqStats,
    tableRowCounts: { 'bq:stream': totalFromBq, packages: totalUpdated },
  })
  log.info(
    { jobId, totalUpdated, bqJobId: bqStats.bqJobId, totalBytesProcessed: bqStats.totalBytesProcessed, totalSlotMs: bqStats.totalSlotMs },
    'dependent_packages_count update complete',
  )
  return { rowsUpdated: totalUpdated }
}
