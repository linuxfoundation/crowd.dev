import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities'

const listActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// processDataset is long-running (10-20 min for ~119MB / ~750K rows).
const processActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 3 },
})

export async function discoverProjects(
  input: { mode: 'incremental' | 'full' } = { mode: 'incremental' },
): Promise<void> {
  const sourceName = 'ossf-criticality-score'

  const { mode } = input

  const allDatasets = await listActivities.listDatasets(sourceName)

  if (allDatasets.length === 0) {
    log.warn('No datasets found. Nothing to process.')
    return
  }

  // allDatasets is sorted newest-first, that is the reason we need the .reverse().
  // Incremental: process only the latest snapshot.
  // Full: process oldest-first so the newest data wins the final upsert.
  const datasets = mode === 'incremental' ? [allDatasets[0]] : [...allDatasets].reverse()

  log.info(`mode=${mode}, ${datasets.length}/${allDatasets.length} datasets to process.`)

  for (let i = 0; i < datasets.length; i++) {
    const dataset = datasets[i]
    log.info(`Processing dataset ${i + 1}/${datasets.length}: ${dataset.id}`)
    await processActivities.processDataset(sourceName, dataset)
  }

  log.info(`Done. Processed ${datasets.length} dataset(s).`)
}
