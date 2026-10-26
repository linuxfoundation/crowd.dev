import { log, proxyActivities } from '@temporalio/workflow'

import type { Channel } from '@crowd/connectors/src/types'

import type * as activities from '../activities/githubLifecycleActivities'
import type {
  IDiscoverFullSyncReposResult,
  IGithubLifecyclePlan,
} from '../activities/githubLifecycleActivities'
import {
  type IGithubLifecycleSummary,
  type ILifecycleSkip,
  githubRepoKey,
} from '../githubLifecycle'
import { chunk } from '../mirrorSweepLimits'
import { pollUntilDrained } from './drain'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: {
    maximumAttempts: 3,
    backoffCoefficient: 2,
  },
})

const longActivity = proxyActivities<
  Pick<typeof activities, 'discoverFullSyncRepos' | 'onboardGithubRepos'>
>({
  startToCloseTimeout: '90 minutes',
  heartbeatTimeout: '5 minutes',
  retry: {
    maximumAttempts: 1,
  },
})

const ONBOARD_BATCH_SIZE = 500

export interface IReconcileGithubIntegrationInput {
  integrationId: string
}

export type ReconcileGithubIntegrationResult = IGithubLifecycleSummary & {
  outcome?: 'not_found' | 'not_github_nango'
}

export async function reconcileGithubIntegration({
  integrationId,
}: IReconcileGithubIntegrationInput): Promise<ReconcileGithubIntegrationResult> {
  const startedAt = Date.now()
  const summary: IGithubLifecycleSummary = {
    integrationId,
    deleted: false,
    desired: 0,
    discovered: 0,
    created: 0,
    unitsInserted: 0,
    deletedUnits: 0,
    repositoriesSoftDeleted: 0,
    undrainedChannels: 0,
    deletesSuppressed: false,
    failed: false,
    skipped: 0,
    durationMs: 0,
  }

  try {
    const discovery = await longActivity.discoverFullSyncRepos({ integrationId })
    const plan = await activity.planGithubLifecycle({ integrationId })
    summary.deleted = plan.deleted
    summary.desired = plan.desired
    summary.discovered = discovery.discovered.length
    summary.deletesSuppressed = plan.deletesSuppressed

    const outcome = ineligibleOutcome(plan)
    if (outcome) {
      log.warn('github lifecycle reconcile skipped', { integrationId, outcome })
      return { ...summary, durationMs: Date.now() - startedAt, outcome }
    }

    await applyPlan(integrationId, plan, discovery, summary)
  } catch (err) {
    summary.failed = true
    summary.durationMs = Date.now() - startedAt
    await activity.logGithubLifecycleSummary(summary)
    throw err
  }

  summary.durationMs = Date.now() - startedAt
  await activity.logGithubLifecycleSummary(summary)
  return summary
}

async function applyPlan(
  integrationId: string,
  plan: IGithubLifecyclePlan,
  discovery: IDiscoverFullSyncReposResult,
  summary: IGithubLifecycleSummary,
): Promise<void> {
  const renamed = renamedKeys(discovery.skipped)
  const creatable = plan.toCreate.filter((key) => !renamed.has(key))
  const toCreate = discovery.renameValidationComplete ? creatable : []
  const alreadySkipped = skippedKeys(discovery.skipped)
  const skipped = [
    ...discovery.skipped,
    ...planSkips(plan),
    ...(discovery.renameValidationComplete
      ? []
      : deferredCreates(creatable.filter((key) => !alreadySkipped.has(key)))),
  ]
  if (skipped.length > 0) {
    await activity.logLifecycleSkips({ integrationId, skipped })
  }
  summary.skipped = skipped.length

  const deleteWork = plan.deleted || plan.toDeleteKeys.length > 0
  if (deleteWork) {
    await deletePhase(integrationId, plan, summary)
  }
  if (plan.deleted || (!deleteWork && toCreate.length === 0)) {
    return
  }

  const deletedDuringCreate =
    toCreate.length > 0 &&
    (await createPhase(integrationId, toCreate, resolvedByKey(discovery.discovered), summary))
  await deleteIfNowDeleted(integrationId, summary, deletedDuringCreate)
}

async function deleteIfNowDeleted(
  integrationId: string,
  summary: IGithubLifecycleSummary,
  deletedDuringCreate: boolean,
): Promise<void> {
  const replanned = await activity.planGithubLifecycle({ integrationId })
  if (replanned.deleted) {
    summary.deleted = true
    await deletePhase(integrationId, replanned, summary)
    return
  }
  if (deletedDuringCreate) {
    log.warn('github lifecycle onboarding stopped: integration is no longer live github-nango', {
      integrationId,
    })
  }
}

function deferredCreates(keys: string[]): ILifecycleSkip[] {
  return keys.map((url) => ({
    url,
    reason: 'incomplete_listing' as const,
    message: 'creation deferred until discovery validates renames',
  }))
}

function skippedKeys(skips: ILifecycleSkip[]): Set<string> {
  return new Set(skips.map(({ url }) => githubRepoKey(url) ?? url.toLowerCase()))
}

function renamedKeys(skips: ILifecycleSkip[]): Set<string> {
  return skippedKeys(skips.filter(({ reason }) => reason === 'repo_renamed'))
}

function ineligibleOutcome(
  plan: IGithubLifecyclePlan,
): ReconcileGithubIntegrationResult['outcome'] {
  if (!plan.found) {
    return 'not_found'
  }
  return plan.githubNango ? undefined : 'not_github_nango'
}

function planSkips(plan: IGithubLifecyclePlan): ILifecycleSkip[] {
  return [
    ...plan.invalidUrls.map((url) => ({ url, reason: 'invalid_url' as const })),
    ...plan.ownedElsewhere.map((url) => ({ url, reason: 'owned_by_other_integration' as const })),
  ]
}

function resolvedByKey(discovered: Channel[]): Record<string, Channel> {
  const resolved: Record<string, Channel> = {}
  for (const channel of discovered) {
    const key = githubRepoKey(channel.channelName)
    if (key !== null) {
      resolved[key] = channel
    }
  }
  return resolved
}

function resolvedFor(keys: string[], resolved: Record<string, Channel>): Record<string, Channel> {
  return Object.fromEntries(
    keys.filter((key) => key in resolved).map((key) => [key, resolved[key]]),
  )
}

async function deletePhase(
  integrationId: string,
  plan: IGithubLifecyclePlan,
  summary: IGithubLifecycleSummary,
): Promise<void> {
  if (plan.deleted) {
    await activity.decommissionAllUnits({ integrationId })
    summary.repositoriesSoftDeleted += await activity.softDeleteIntegrationRepositories({
      integrationId,
    })
  } else {
    await activity.decommissionChannels({ integrationId, channelKeys: plan.toDeleteKeys })
  }
  await pollUntilDrained(async () => (await activity.countUndrainedUnits({ integrationId })) === 0)
  summary.deletedUnits += await activity.deleteDrainedUnits({ integrationId })

  const undrained = await activity.listUndeletedChannels({ integrationId })
  if (undrained.length > 0) {
    await activity.logUndrainedChannels({ integrationId, channelKeys: undrained })
  }
  summary.undrainedChannels = undrained.length
}

async function createPhase(
  integrationId: string,
  toCreate: string[],
  resolved: Record<string, Channel>,
  summary: IGithubLifecycleSummary,
): Promise<boolean> {
  for (const [batchIndex, keys] of chunk(toCreate, ONBOARD_BATCH_SIZE).entries()) {
    const onboarded = await longActivity.onboardGithubRepos({
      integrationId,
      keys,
      resolved: resolvedFor(keys, resolved),
      startIndex: batchIndex * ONBOARD_BATCH_SIZE,
    })
    summary.created += onboarded.created
    summary.unitsInserted += onboarded.unitsInserted
    summary.skipped += onboarded.skipped
    if (onboarded.integrationDeleted) {
      summary.skipped += Math.max(toCreate.length - (batchIndex + 1) * ONBOARD_BATCH_SIZE, 0)
      return true
    }
  }
  return false
}
