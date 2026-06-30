import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

// Packages fetched per batch (one sequential, throttled activity call) and batches
// per workflow run before continueAsNew resets history. ~INGEST_BATCH × ROUNDS_PER_RUN
// packages are drained per run.
const INGEST_BATCH = 50
const ROUNDS_PER_RUN = 20

interface IngestState {
  cursor: string
}

// Drain critical PyPI packages due for a metadata run (never scanned, or stale),
// keyset-paginated on purl. A short batch (< INGEST_BATCH) means the due set after the
// cursor is exhausted, so the run ends; otherwise continueAsNew carries the cursor.
// The daily schedule re-selects newly-due packages on the next run.
export async function ingestPypiPackages(state: IngestState = { cursor: '' }): Promise<void> {
  let cursor = state.cursor

  const stopAfterFirstPage = await acts.pypiStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const { purls, nextCursor } = await acts.getUnscannedPypiBatch(cursor, INGEST_BATCH)
    if (purls.length === 0) return
    await acts.ingestPypiPackageBatch(purls)
    cursor = nextCursor
    if (stopAfterFirstPage) return
    if (purls.length < INGEST_BATCH) return
  }

  await continueAsNew<typeof ingestPypiPackages>({ cursor })
}
