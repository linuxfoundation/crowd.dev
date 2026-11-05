import {
  ActivityFailure,
  CancellationScope,
  ParentClosePolicy,
  continueAsNew,
  log,
  proxyActivities,
  startChild,
  workflowInfo,
} from '@temporalio/workflow'

import type { InferredRepoMemberContributorsBody } from '@crowd/data-access-layer/src/osspckgs/memberContributorsBody'

import type * as memberContributorActivities from './activities'
import { addChunkCounts, emptyChunkCounts } from './affiliations/counts'
import type { IdentityAffiliationsRunCounts, IdentityAffiliationsState } from './affiliations/state'
import type {
  GitActivitySyncCounts,
  GitActivitySyncOptions,
} from './git-activity/syncGitActivityContributors'
import { type GithubSyncCounts, addGithubSyncCounts, emptyGithubSyncCounts } from './github/counts'
import type { GovernanceSyncCounts } from './governance/syncGovernanceContributors'
import type { MemberContributorsJobRef } from './job/start'

export type { MemberContributorsJobRef }

const RETRY = {
  initialInterval: '30 seconds',
  backoffCoefficient: 2,
  maximumAttempts: 3,
}

const UNBOUNDED_RETRY = {
  initialInterval: RETRY.initialInterval,
  backoffCoefficient: RETRY.backoffCoefficient,
  maximumInterval: '5 minutes',
}

const { syncGovernanceContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '5 minutes',
  retry: RETRY,
})

const { syncGitActivityContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '4 hours',
  heartbeatTimeout: '10 minutes',
  retry: RETRY,
})

const { syncGithubContributorsBatch } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '2 hours',
  heartbeatTimeout: '5 minutes',
  retry: RETRY,
})

const { resolveIdentityAffiliationsChunk } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '10 minutes',
  retry: RETRY,
})

const { prepareIdentityAffiliationsRun } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '2 hours',
  retry: RETRY,
})

const { mergeIdentityAffiliations } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 1 },
})

const { startMemberContributorsJob } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '5 minutes',
  retry: RETRY,
})

const { collectGithubRepoContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '5 minutes',
  retry: RETRY,
})

const { resolveNewIdentityAffiliations } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '10 minutes',
  retry: RETRY,
})

const { completeMemberContributorsJob, failMemberContributorsJob } = proxyActivities<
  typeof memberContributorActivities
>({
  startToCloseTimeout: '1 minute',
  retry: UNBOUNDED_RETRY,
})

const { startInferredRepoMemberContributorsJob } = proxyActivities<
  typeof memberContributorActivities
>({
  startToCloseTimeout: '5 minutes',
  retry: RETRY,
})

const { resolveRepoViaLlm } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '3 minutes',
  retry: { ...RETRY, maximumAttempts: 2 },
})

const { fetchGithubRepoContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '5 minutes',
  retry: RETRY,
})

const { buildInferredRepoMemberContributors } = proxyActivities<typeof memberContributorActivities>(
  {
    startToCloseTimeout: '5 minutes',
    retry: RETRY,
  },
)

const { markMemberContributorsJobUnresolvable } = proxyActivities<
  typeof memberContributorActivities
>({
  startToCloseTimeout: '1 minute',
  retry: UNBOUNDED_RETRY,
})

const GITHUB_BATCH_SIZE = 500
const GITHUB_BATCHES_PER_RUN = 20

const AFFILIATIONS_PAGE_SIZE = 5000
const AFFILIATIONS_PAGES_PER_RUN = 40

export interface GithubSyncState {
  runStartedAt: string
  counts: GithubSyncCounts
}

export async function syncGovernanceFileContributors(): Promise<GovernanceSyncCounts> {
  return syncGovernanceContributors()
}

export async function syncRepoContributorsFromGitActivity(
  options: GitActivitySyncOptions,
): Promise<GitActivitySyncCounts> {
  const counts = await syncGitActivityContributors(options)
  await startIdentityAffiliationsChild()
  return counts
}

async function startIdentityAffiliationsChild(): Promise<void> {
  try {
    await startChild(resolveIdentityAffiliations, {
      workflowId: 'identity-affiliations-daily',
      taskQueue: 'security-contacts-worker',
      parentClosePolicy: ParentClosePolicy.ABANDON,
      workflowExecutionTimeout: '6 hours',
    })
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      log.info('identity-affiliations-daily is already running, skipping start')
      return
    }
    throw err
  }
}

export async function syncRepoContributorsFromGithub(
  state: GithubSyncState = {
    runStartedAt: workflowInfo().startTime.toISOString(),
    counts: emptyGithubSyncCounts(),
  },
): Promise<GithubSyncCounts> {
  let counts = state.counts

  for (let batch = 0; batch < GITHUB_BATCHES_PER_RUN; batch++) {
    const result = await syncGithubContributorsBatch({
      runStartedAt: state.runStartedAt,
      limit: GITHUB_BATCH_SIZE,
    })
    counts = addGithubSyncCounts(counts, result.counts)
    if (result.counts.selected < GITHUB_BATCH_SIZE) return counts
  }

  return continueAsNew<typeof syncRepoContributorsFromGithub>({
    runStartedAt: state.runStartedAt,
    counts,
  })
}

async function startIdentityAffiliationsRun(): Promise<IdentityAffiliationsState> {
  const { runStartedAt, identities } = await prepareIdentityAffiliationsRun()
  return { runStartedAt, identities, afterKey: null, counts: emptyChunkCounts() }
}

export async function resolveIdentityAffiliations(
  state?: IdentityAffiliationsState,
): Promise<IdentityAffiliationsRunCounts> {
  let current = state ?? (await startIdentityAffiliationsRun())

  for (let page = 0; page < AFFILIATIONS_PAGES_PER_RUN; page++) {
    const result = await resolveIdentityAffiliationsChunk({
      runStartedAt: current.runStartedAt,
      afterKey: current.afterKey,
      limit: AFFILIATIONS_PAGE_SIZE,
    })
    current = {
      ...current,
      afterKey: result.lastKey ?? current.afterKey,
      counts: addChunkCounts(current.counts, result.counts),
    }
    if (result.read < AFFILIATIONS_PAGE_SIZE) {
      const merged = await mergeIdentityAffiliations({ runStartedAt: current.runStartedAt })
      return { ...merged, identities: current.identities }
    }
  }

  return continueAsNew<typeof resolveIdentityAffiliations>(current)
}

function failureReason(err: unknown): string {
  const cause = err instanceof ActivityFailure ? err.cause : err
  return cause instanceof Error ? cause.message : String(err)
}

export async function collectMemberContributors(input: MemberContributorsJobRef): Promise<void> {
  try {
    const repo = await startMemberContributorsJob(input)
    const collected = await collectGithubRepoContributors(repo)
    await resolveNewIdentityAffiliations({ logins: collected.logins })
    await completeMemberContributorsJob(input)
  } catch (err) {
    await CancellationScope.nonCancellable(() =>
      failMemberContributorsJob({ jobId: input.jobId, reason: failureReason(err) }),
    )
    throw err
  }
}

export async function collectMemberContributorsFromInferredRepo(
  input: MemberContributorsJobRef,
): Promise<InferredRepoMemberContributorsBody | undefined> {
  try {
    const { purl, ecosystem } = await startInferredRepoMemberContributorsJob(input)
    const resolved = await resolveRepoViaLlm({ jobId: input.jobId, purl })
    if (resolved.outcome === 'unresolvable') {
      await markMemberContributorsJobUnresolvable({
        jobId: input.jobId,
        reason: resolved.reason,
        repoUrl: resolved.repoUrl,
      })
      return undefined
    }
    const collected = await fetchGithubRepoContributors({ repoUrl: resolved.repoUrl })
    await resolveNewIdentityAffiliations({ logins: collected.logins })
    const body = await buildInferredRepoMemberContributors({
      purl,
      ecosystem,
      repoUrl: resolved.repoUrl,
      logins: collected.logins,
    })
    await completeMemberContributorsJob(input)
    return body
  } catch (err) {
    await CancellationScope.nonCancellable(() =>
      failMemberContributorsJob({ jobId: input.jobId, reason: failureReason(err) }),
    )
    throw err
  }
}
