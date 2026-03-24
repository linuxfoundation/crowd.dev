import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import * as activities from '../../activities'
import { IScriptBatchTestArgs } from '../../types'
import { chunkArray } from '../../utils/common'

const {
  startOrphanCleanupRun,
  updateOrphanCleanupRun,
  getOrphanMembersSegmentsAgg,
  deleteOrphanMembersSegmentsAgg,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 3 },
})

export async function cleanupMemberSegmentsAgg(args: IScriptBatchTestArgs): Promise<void> {
  const BATCH_SIZE = args.batchSize ?? 100
  const AGGREGATE_NAME = 'memberSegmentsAgg'

  let runId: string | undefined
  // Use cumulative counters from previous iterations or start from 0
  let totalOrphansFound = args.cumulativeOrphansFound ?? 0
  let totalOrphansDeleted = args.cumulativeOrphansDeleted ?? 0
  const startTime = args.workflowStartTime ?? Date.now()

  try {
    // Initialize the cleanup run only on the first iteration
    if (!args.cleanupRunId) {
      runId = await startOrphanCleanupRun(AGGREGATE_NAME)
    } else {
      runId = args.cleanupRunId
    }

    // Get orphaned records
    const orphanIds = await getOrphanMembersSegmentsAgg(BATCH_SIZE)

    if (orphanIds.length === 0) {
      // Update the cleanup run as completed
      if (runId) {
        await updateOrphanCleanupRun(runId, {
          completedAt: new Date(),
          status: 'completed',
          executionTimeMs: Date.now() - startTime,
        })
      }

      return
    }

    const batchOrphansFound = orphanIds.length
    totalOrphansFound += batchOrphansFound

    // Process orphans in chunks
    const CHUNK_SIZE = 25
    let batchDeletedCount = 0

    for (const chunk of chunkArray(orphanIds, CHUNK_SIZE)) {
      const deleteTasks = chunk.map(async (id) => {
        await deleteOrphanMembersSegmentsAgg(id)
        batchDeletedCount++
      })

      await Promise.all(deleteTasks).catch((err) => {
        console.error(`Error cleaning up orphan ${AGGREGATE_NAME} records!`, err)
        throw err
      })
    }

    totalOrphansDeleted += batchDeletedCount

    // Update the cleanup run with incremental progress
    if (runId) {
      await updateOrphanCleanupRun(runId, {
        incrementOrphansFound: batchOrphansFound,
        incrementOrphansDeleted: batchDeletedCount,
      })
    }

    // Continue as new for the next batch, passing cumulative totals
    await continueAsNew<typeof cleanupMemberSegmentsAgg>({
      ...args,
      cleanupRunId: runId,
      cumulativeOrphansFound: totalOrphansFound,
      cumulativeOrphansDeleted: totalOrphansDeleted,
      workflowStartTime: startTime,
    })
  } catch (error) {
    // Update the cleanup run as failed
    if (runId) {
      await updateOrphanCleanupRun(runId, {
        completedAt: new Date(),
        status: 'failed',
        errorMessage: error.message || String(error),
        executionTimeMs: Date.now() - startTime,
      })
    }

    throw error
  }
}
