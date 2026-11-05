import { ParentClosePolicy, executeChild, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/switchSyncPipelineActivities'
import type { ISweepRepo, ISwitchSyncResult } from '../syncPipelineSwitch'
import { switchSyncPipeline, switchSyncPipelineWorkflowId } from './switchSyncPipeline'

const ALREADY_STARTED_ERROR = 'WorkflowExecutionAlreadyStartedError'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: {
    maximumAttempts: 3,
    backoffCoefficient: 2,
  },
})

export interface ISwitchSweepSummary {
  repos: number
  switched: number
  refused: number
  unchanged: number
  skipped: number
  failed: number
}

export async function switchSyncPipelineSweep(): Promise<ISwitchSweepSummary> {
  const repos = await activity.listSwitchSweepRepos()
  const summary: ISwitchSweepSummary = {
    repos: repos.length,
    switched: 0,
    refused: 0,
    unchanged: 0,
    skipped: 0,
    failed: 0,
  }

  for (const repo of repos) {
    const results = await switchRepo(repo)
    if (results === 'skipped' || results === 'failed') {
      summary[results] += 1
      continue
    }
    for (const result of results) {
      summary[result.outcome] += 1
    }
  }

  log.info('sync pipeline sweep complete', { ...summary })
  return summary
}

async function switchRepo(repo: ISweepRepo): Promise<ISwitchSyncResult[] | 'skipped' | 'failed'> {
  try {
    return await executeChild(switchSyncPipeline, {
      workflowId: switchSyncPipelineWorkflowId(repo.owner, repo.repo),
      args: [{ ...repo, direction: 'connectors' }],
      parentClosePolicy: ParentClosePolicy.ABANDON,
    })
  } catch (err) {
    if (err instanceof Error && err.name === ALREADY_STARTED_ERROR) {
      log.warn('sweep skipped a repo with a switch already running', { ...repo })
      return 'skipped'
    }
    log.error('sweep child switch failed', { ...repo, err })
    return 'failed'
  }
}
