import { markJobStatus } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('mergeStagingToTable')

export interface MergeStagingInput {
  jobId: number
  // Setup SQL that runs before the counted INSERT statements (e.g. SET LOCAL work_mem, CREATE TEMP TABLE).
  // These do not contribute to rowsAffected.
  prepareSql?: string | string[]
  mergeSql: string | string[]
  // Table name for each SQL statement (parallel array). Used for per-table row count tracking.
  tableNames: string | string[]
  // Set false for intermediate chunks — skips marking job done until the final merge.
  isFinal?: boolean
  // Row count merged in prior chunks, added to this chunk's count when marking done.
  priorRowsAffected?: number
  // For log progress: { index: chunkIndex, total: totalChunks }
  chunkInfo?: { index: number; total: number }
}

export interface MergeStagingOutput {
  rowsAffected: number
}

export async function mergeStagingToTable(input: MergeStagingInput): Promise<MergeStagingOutput> {
  const { jobId, prepareSql, mergeSql, tableNames, isFinal = true, priorRowsAffected = 0, chunkInfo } = input
  const prepareStatements = prepareSql ? (Array.isArray(prepareSql) ? prepareSql : [prepareSql]) : []
  const statements = Array.isArray(mergeSql) ? mergeSql : [mergeSql]
  const names = Array.isArray(tableNames) ? tableNames : [tableNames]

  const qx = await getPackagesDb()

  await markJobStatus(qx, jobId, 'merging')

  let rowsAffected = 0
  const tableRowCounts: Record<string, number> = {}

  await qx.tx(async (tx) => {
    for (const sql of prepareStatements) {
      await tx.result(sql)
    }
    for (let i = 0; i < statements.length; i++) {
      const count = await tx.result(statements[i])
      rowsAffected += count
      const table = names[i] ?? `table_${i}`
      tableRowCounts[table] = (tableRowCounts[table] ?? 0) + count
    }
  })

  if (chunkInfo) {
    log.info(
      { jobId, rowsAffected, tableRowCounts, chunk: `${chunkInfo.index + 1}/${chunkInfo.total}` },
      'Chunk merge complete',
    )
  }

  if (!isFinal) {
    await markJobStatus(qx, jobId, 'merging', {
      rowCountPg: priorRowsAffected + rowsAffected,
    })
  }

  if (isFinal) {
    const totalRowsAffected = priorRowsAffected + rowsAffected
    await markJobStatus(qx, jobId, 'done', {
      finishedAt: new Date(),
      rowCountPg: totalRowsAffected,
      tableRowCounts,
    })
    log.info({ jobId, rowsAffected: totalRowsAffected, tableRowCounts }, 'Merge complete')
  }

  return { rowsAffected }
}
