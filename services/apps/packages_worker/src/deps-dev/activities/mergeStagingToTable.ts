import { markJobStatus } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('mergeStagingToTable')

export interface MergeStagingInput {
  jobId: number
  mergeSql: string | string[]
}

export interface MergeStagingOutput {
  rowsAffected: number
}

export async function mergeStagingToTable(input: MergeStagingInput): Promise<MergeStagingOutput> {
  const { jobId, mergeSql } = input
  const statements = Array.isArray(mergeSql) ? mergeSql : [mergeSql]

  const qx = await getPackagesDb()

  await markJobStatus(qx, jobId, 'merging')

  let rowsAffected = 0
  await qx.tx(async (tx) => {
    for (const sql of statements) {
      rowsAffected += await tx.result(sql)
    }
  })

  await markJobStatus(qx, jobId, 'done', {
    finishedAt: new Date(),
    rowCountPg: rowsAffected,
  })

  log.info({ jobId, rowsAffected }, 'Merge complete')

  return { rowsAffected }
}
