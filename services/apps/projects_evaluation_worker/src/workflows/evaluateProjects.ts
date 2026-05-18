import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities'

// Short timeout: just a DB read.
const fetchActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// Each AI evaluation call takes ~30-40s; give generous headroom per project.
const evaluateActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '3 minutes',
  retry: { maximumAttempts: 2 },
})

const DEFAULT_BATCH_SIZE = 100

export async function evaluateProjects(
  input: { batchSize?: number } = {},
): Promise<void> {
  const batchSize = input.batchSize ?? DEFAULT_BATCH_SIZE

  log.info('evaluateProjects workflow started.')

  const projects = await fetchActivities.fetchPendingProjects(batchSize)

  if (projects.length === 0) {
    log.info('No projects pending evaluation. Nothing to do.')
    return
  }

  log.info(`Evaluating ${projects.length} project(s) (batch size: ${batchSize}).`)

  let succeeded = 0
  let failed = 0

  for (let i = 0; i < projects.length; i++) {
    const project = projects[i]
    log.info(`[${i + 1}/${projects.length}] Evaluating: ${project.repoUrl}`)

    try {
      await evaluateActivities.evaluateAndUpdateProject(project)
      succeeded++
    } catch (err) {
      // Log and continue — a single failure should not abort the whole batch.
      failed++
      log.error(`Evaluation failed for project id=${project.id} repoUrl=${project.repoUrl}: ${String(err)}`)
    }
  }

  log.info(`Batch evaluation complete. total=${projects.length} succeeded=${succeeded} failed=${failed}`)
}
