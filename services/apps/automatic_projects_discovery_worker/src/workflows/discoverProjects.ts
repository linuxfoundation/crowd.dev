import {
  CancellationScope,
  isCancellation,
  log,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import type * as activities from '../activities'

const listActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// processDataset is long-running: ~119MB / ~750K rows + inter-page throttle can exceed 60 min.
const processActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '90 minutes',
  heartbeatTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
})

const pipelineRunActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const watermarkActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

// Only these sources are watermarked today. lf-criticality-score keeps doing a full
// fetch every run — its interface accepts `since` but the workflow never supplies one.
const WATERMARKED_SOURCES = ['insights-discussions']

interface ISourceBreakdown {
  rows: number
  skippedPreCheck: number
  skippedAlreadyInCdp: number
  skipped: number
  accepted: number
}

export async function discoverProjects(
  input: { mode: 'incremental' | 'full' } = { mode: 'incremental' },
): Promise<void> {
  const { mode } = input
  const { workflowId, runId } = workflowInfo()

  const pipelineRunId = await CancellationScope.nonCancellable(() =>
    pipelineRunActivities.startDiscoveryPipelineRun(workflowId, runId),
  )

  let totalCandidates = 0
  let succeeded = 0
  let failed = 0
  let skippedPreCheck = 0
  let skippedAlreadyInCdp = 0
  let skipped = 0
  const bySource: Record<string, ISourceBreakdown> = {}

  try {
    const sourceNames = await listActivities.listSources()

    for (const sourceName of sourceNames) {
      const watermarked = WATERMARKED_SOURCES.includes(sourceName)
      let capturedAt: string | undefined
      let since: string | undefined

      if (watermarked) {
        const watermark = await watermarkActivities.readSourceWatermark(sourceName)
        capturedAt = watermark.capturedAt
        since = mode === 'incremental' ? (watermark.since ?? undefined) : undefined
      }

      let allDatasets: Awaited<ReturnType<typeof listActivities.listDatasets>>
      try {
        allDatasets = await listActivities.listDatasets(sourceName, since)
      } catch (err) {
        if (isCancellation(err)) {
          throw err
        }
        failed++
        log.error(`Listing datasets failed for source=${sourceName}: ${String(err)}`)
        continue
      }

      if (allDatasets.length === 0) {
        log.warn(`No datasets found for source "${sourceName}". Skipping.`)
        continue
      }

      // allDatasets is sorted newest-first.
      // Incremental: process only the latest snapshot. Full: process newest-first too, since processDataset only inserts repoUrls
      const datasets = mode === 'incremental' ? [allDatasets[0]] : allDatasets

      log.info(
        `source=${sourceName} mode=${mode}, ${datasets.length}/${allDatasets.length} datasets to process.`,
      )

      const sourceStats: ISourceBreakdown = {
        rows: 0,
        skippedPreCheck: 0,
        skippedAlreadyInCdp: 0,
        skipped: 0,
        accepted: 0,
      }

      let sourceOk = true
      let sourceTruncated = false

      for (let i = 0; i < datasets.length; i++) {
        const dataset = datasets[i]
        log.info(`[${sourceName}] Processing dataset ${i + 1}/${datasets.length}: ${dataset.id}`)

        try {
          const result = await processActivities.processDataset(sourceName, dataset)

          const datasetSkipped =
            result.totalRows -
            result.totalAccepted -
            result.totalSkipped -
            result.totalSkippedAlreadyInCdp

          totalCandidates += result.totalRows
          succeeded += result.totalAccepted
          skippedPreCheck += result.totalSkipped
          skippedAlreadyInCdp += result.totalSkippedAlreadyInCdp
          skipped += datasetSkipped

          sourceStats.rows += result.totalRows
          sourceStats.skippedPreCheck += result.totalSkipped
          sourceStats.skippedAlreadyInCdp += result.totalSkippedAlreadyInCdp
          sourceStats.skipped += datasetSkipped
          sourceStats.accepted += result.totalAccepted

          if (result.truncated) {
            sourceTruncated = true
          }
        } catch (err) {
          if (isCancellation(err)) {
            throw err
          }

          failed++
          sourceOk = false
          log.error(
            `Dataset processing failed for source=${sourceName} datasetId=${dataset.id}: ${String(err)}`,
          )
        }
      }

      bySource[sourceName] = sourceStats

      if (watermarked && capturedAt && sourceOk && !sourceTruncated) {
        await watermarkActivities.commitSourceWatermark(sourceName, capturedAt, mode === 'full')
      }

      log.info(`[${sourceName}] Done. Processed ${datasets.length} dataset(s).`)
    }

    log.info(
      `Discovery run complete. totalCandidates=${totalCandidates} succeeded=${succeeded} failed=${failed} skipped=${skipped} skippedPreCheck=${skippedPreCheck} skippedAlreadyInCdp=${skippedAlreadyInCdp}`,
    )

    await pipelineRunActivities.finishDiscoveryPipelineRun(pipelineRunId, {
      status: 'completed',
      totalCandidates,
      succeeded,
      failed,
      skipped,
      skippedPreCheck,
      details: bySource,
    })
  } catch (err) {
    await CancellationScope.nonCancellable(() =>
      pipelineRunActivities.finishDiscoveryPipelineRun(pipelineRunId, {
        status: 'failed',
        totalCandidates,
        succeeded,
        failed,
        skipped,
        skippedPreCheck,
        details: bySource,
        errorMessage: String(err),
      }),
    )
    throw err
  }
}
