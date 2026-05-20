import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities'
import type { IEvaluateProjectsInput, IPriorityConfig } from '../types'

// Quick DB write — promote auto → evaluate.
const promotionActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

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

const DEFAULT_PRIORITY_CONFIG: IPriorityConfig = {
  evaluateLimit: 50,
  sourcePriority: ['insights-discussions'],
}

export async function evaluateProjects(input: IEvaluateProjectsInput = {}): Promise<void> {
  const { batchSize = 50, priorityConfig = DEFAULT_PRIORITY_CONFIG } = input

  log.info('evaluateProjects workflow started.')

  // Step 1: promote 'auto' projects to 'evaluate' according to priority config.
  await promotionActivities.promoteProjectsForEvaluation(priorityConfig)

  // Step 2: fetch the evaluation queue (includes any leftovers from prior runs).
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
      log.error(
        `Evaluation failed for project id=${project.id} repoUrl=${project.repoUrl}: ${String(err)}`,
      )
    }
  }

  log.info(
    `Batch evaluation complete. total=${projects.length} succeeded=${succeeded} failed=${failed}`,
  )
}
