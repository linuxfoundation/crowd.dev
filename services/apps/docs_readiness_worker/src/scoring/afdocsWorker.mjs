// Runs in a worker thread (see runChecksIsolated.ts); plain ESM so it needs no TS loader.
import { parentPort, workerData } from 'node:worker_threads'

import { runChecks } from 'afdocs'

try {
  parentPort.postMessage({ result: await runChecks(workerData.url) })
} catch (err) {
  parentPort.postMessage({ error: err instanceof Error ? err.message : String(err) })
}
