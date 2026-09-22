import type { BigQuery } from '@google-cloud/bigquery'
import type { Job } from '@google-cloud/bigquery'

import type { BqStats } from '@crowd/data-access-layer'

export async function extractBqStats(
  job: Job,
  bqClient?: BigQuery,
): Promise<BqStats & { bqJobId: string; bqBytesBilled: number }> {
  const [metadata] = await job.getMetadata()
  const stats = (metadata.statistics ?? {}) as Record<string, unknown>
  const query = (stats.query ?? {}) as Record<string, unknown>

  const creationTime = stats.creationTime ? Number(stats.creationTime) : undefined
  const startTime = stats.startTime ? Number(stats.startTime) : undefined
  const endTime = stats.endTime ? Number(stats.endTime) : undefined

  const referencedTables = Array.isArray(query.referencedTables)
    ? (query.referencedTables as Array<Record<string, string>>).map(
        (t) => `${t.projectId}.${t.datasetId}.${t.tableId}`,
      )
    : []

  const totalBytesProcessed = Number(stats.totalBytesProcessed ?? 0)
  // For script jobs, totalBytesBilled is in query.totalBytesBilled, not stats.totalBytesBilled.
  const totalBytesBilled = Number(query.totalBytesBilled ?? stats.totalBytesBilled ?? 0)
  const totalSlotMs = Number(query.totalSlotMs ?? 0)
  let outputRows: number | undefined =
    query.outputRows != null ? Number(query.outputRows) : undefined

  // Script jobs (CREATE TEMP TABLE + EXPORT DATA): scriptStatistics is not returned by getMetadata().
  // List child jobs instead — the CREATE TEMP TABLE child has the row count in dmlStats.insertedRowCount.
  const numChildJobs = Number(stats.numChildJobs ?? 0)
  if (numChildJobs > 0 && outputRows == null && bqClient) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [childJobs] = await bqClient.getJobs({ parentJobId: job.id, location: 'US' } as any)
      for (const childJob of childJobs) {
        const [childMeta] = await childJob.getMetadata()
        const childQuery = (childMeta.statistics?.query ?? {}) as Record<string, unknown>
        // EXPORT_DATA child job exposes row count via exportDataStatistics.rowCount
        const exportStats = (childQuery.exportDataStatistics ?? {}) as Record<string, unknown>
        if (exportStats.rowCount != null) {
          outputRows = Number(exportStats.rowCount)
          break
        }
      }
    } catch {
      // non-fatal — outputRows stays undefined
    }
  }

  return {
    bqJobId: job.id ?? '',
    bqBytesBilled: totalBytesBilled,
    jobId: job.id ?? '',
    cacheHit: Boolean(query.cacheHit ?? false),
    totalBytesProcessed,
    totalBytesBilled,
    totalSlotMs,
    outputRows,
    durationMs: startTime != null && endTime != null ? endTime - startTime : undefined,
    referencedTables,
    creationTime,
    startTime,
    endTime,
  }
}
