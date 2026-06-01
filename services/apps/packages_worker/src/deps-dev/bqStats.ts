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
  const totalBytesBilled = Number(stats.totalBytesBilled ?? 0)
  const totalSlotMs = Number(query.totalSlotMs ?? 0)
  let outputRows: number | undefined = query.outputRows != null ? Number(query.outputRows) : undefined

  // Script jobs (CREATE TEMP TABLE + EXPORT DATA) have per-statement stats in child jobs.
  // Pull outputRows from the CREATE TEMP TABLE child, which has it as a standard query result.
  const scriptStats = stats.scriptStatistics as Record<string, unknown> | undefined
  if (scriptStats && bqClient) {
    const stackFrames = (scriptStats.stackFrames ?? []) as Array<Record<string, unknown>>
    const createFrame = stackFrames.find((f) =>
      String(f.text ?? '')
        .trimStart()
        .toUpperCase()
        .startsWith('CREATE TEMP TABLE'),
    )
    if (createFrame?.childJobId) {
      try {
        // location required — jobs created with location:'US' are not found without it
        const childJob = bqClient.job(String(createFrame.childJobId), { location: 'US' })
        const [childMeta] = await childJob.getMetadata()
        const childQuery = (childMeta.statistics?.query ?? {}) as Record<string, unknown>
        if (childQuery.outputRows != null) outputRows = Number(childQuery.outputRows)
      } catch {
        // non-fatal — outputRows stays undefined
      }
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
