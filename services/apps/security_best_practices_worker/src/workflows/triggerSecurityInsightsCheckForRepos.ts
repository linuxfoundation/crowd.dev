import {
  ApplicationFailure,
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  continueAsNew,
  executeChild,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'
import { ITokenInfo, ITriggerSecurityInsightsCheckForReposParams } from '../types'

import { upsertOSPSBaselineSecurityInsights } from './upsertOSPSBaselineSecurityInsights'

const { findObsoleteRepos, initializeTokenInfos, updateTokenInfos } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 3 },
})

const ONE_HOUR_MS = 60 * 60 * 1000

export async function triggerSecurityInsightsCheckForRepos(
  args: ITriggerSecurityInsightsCheckForReposParams,
): Promise<void> {
  const REPOS_OBSOLETE_AFTER_SECONDS = 30 * 24 * 60 * 60 // 30 days
  const LIMIT_REPOS_TO_CHECK_PER_RUN = 100
  const MAX_PARALLEL_CHILDREN = 5
  const MAX_TOKEN_ATTEMPTS = 5

  const info = workflowInfo()
  const failedRepoUrls = args?.failedRepoUrls || []

  // Use workflowInfo().startTime as the replay-safe time reference — Temporal workflows must
  // be deterministic, so Date.now()/new Date() are banned by services-checklist.md:9-12.
  // startTime is stable within a run and advances on each continueAsNew batch.
  const nowMs = info.startTime.getTime()
  const nowIso = info.startTime.toISOString()

  // Token state is persisted in Redis via updateTokenInfos so isRateLimited (with rateLimitedAt
  // for expiry) and isInvalid survive across continueAsNew batches.
  const tokenInfos: ITokenInfo[] = await initializeTokenInfos()

  const repos = await findObsoleteRepos(
    REPOS_OBSOLETE_AFTER_SECONDS,
    failedRepoUrls,
    LIMIT_REPOS_TO_CHECK_PER_RUN,
  )

  if (repos.length === 0) {
    return
  }

  const queue = [...repos]
  const activeTasks: Promise<void>[] = []

  async function processRepo(repo: (typeof repos)[0]): Promise<void> {
    let attempts = 0
    while (attempts < MAX_TOKEN_ATTEMPTS) {
      const tokenInfo = getNextToken(tokenInfos, nowMs)
      if (!tokenInfo) {
        // Distinguish: are there tokens that will become free, or are all excluded?
        const anyRecoverable = tokenInfos.some((t) => !t.isInvalid && !t.isRateLimited)
        if (anyRecoverable) {
          // All non-excluded tokens are currently in use by other concurrent tasks.
          // Put the repo back and let the outer loop retry after a task finishes.
          queue.unshift(repo)
        } else {
          console.error(`All tokens excluded for repo ${repo.repoUrl}, skipping`)
          failedRepoUrls.push(repo.repoUrl)
        }
        break
      }
      const token = tokenInfo.token

      await acquireToken(tokenInfos, token)

      try {
        await executeChild(upsertOSPSBaselineSecurityInsights, {
          workflowId: `${info.workflowId}->${repo.repoUrl}->attempt${attempts}`,
          cancellationType: ChildWorkflowCancellationType.ABANDON,
          parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
          retry: {
            maximumAttempts: 3,
            initialInterval: 2000,
            backoffCoefficient: 2,
            maximumInterval: 30000,
            nonRetryableErrorTypes: ['Token403Error', 'TokenAuthError'],
          },
          args: [
            {
              repoUrl: repo.repoUrl,
              insightsProjectId: repo.insightsProjectId,
              insightsProjectSlug: repo.insightsProjectSlug,
              token,
            },
          ],
        })
        return // success
      } catch (error) {
        // executeChild rejects with ChildWorkflowFailure wrapping ActivityFailure wrapping
        // ApplicationFailure — traverse the cause chain to find the root ApplicationFailure
        const appFailure = unwrapApplicationFailure(error)
        if (appFailure?.type === 'Token403Error') {
          tokenInfo.isRateLimited = true
          tokenInfo.rateLimitedAt = nowIso
          attempts++
          continue // retry with a different token
        } else if (appFailure?.type === 'TokenAuthError') {
          console.error(
            `Token invalid/expired for repo ${repo.repoUrl}, permanently excluding token`,
          )
          tokenInfo.isInvalid = true
          attempts++
          continue // retry with a different token
        } else {
          console.error(`Failed to process repo ${repo.repoUrl}:`, error)
          // we retried this error using the retry policy (because it's not a token non-retryable error)
          // but it failed in all retries.
          // to proceed with processing, we don't wanna try this repo again in this run
          failedRepoUrls.push(repo.repoUrl)
          break
        }
      } finally {
        await releaseToken(tokenInfos, tokenInfo.token)
      }
    }

    if (attempts >= MAX_TOKEN_ATTEMPTS) {
      console.error(`Exhausted token attempts for repo ${repo.repoUrl}, skipping`)
      failedRepoUrls.push(repo.repoUrl)
    }
  }

  /**
   * We fire up to MAX_PARALLEL_CHILDREN concurrent tasks to process the repos.
   * Each task will acquire a token, process the repo, and then release the token.
   * If a task fails with a Token403Error, we will retry it with a different token.
   * When a task finishes (checked through Promise.race),
   * it will be removed from the activeTasks array and the next task will be started.
   * This way we don't need to wait for all tasks to finish before starting new ones.
   */
  let deferred = false
  while (queue.length > 0 || activeTasks.length > 0) {
    while (queue.length > 0 && activeTasks.length < MAX_PARALLEL_CHILDREN) {
      // Only start a task if a token is currently free — avoids false "no token" failures
      // when all tokens are temporarily held by other concurrent tasks.
      refreshExpiredRateLimits(tokenInfos, nowMs)
      const hasFreeToken = tokenInfos.some((t) => !t.isInvalid && !t.isRateLimited && !t.inUse)
      if (!hasFreeToken) break

      const repo = queue.shift()
      const task = processRepo(repo).finally(() => {
        const index = activeTasks.indexOf(task)
        if (index >= 0) activeTasks.splice(index, 1)
      })
      activeTasks.push(task)
    }

    if (activeTasks.length > 0) {
      await Promise.race(activeTasks)
    } else if (queue.length > 0) {
      // No active tasks and no free tokens — remaining repos can't be processed this batch.
      // Don't add them to failedRepoUrls: they're still obsolete and findObsoleteRepos will
      // pick them up on the next batch after rate limits expire.
      console.warn(
        `No tokens available; deferring ${queue.length} repos to next batch (they will be re-fetched)`,
      )
      deferred = true
      break
    }
  }

  // Persist token state to Redis so isRateLimited (with rateLimitedAt for expiry) and isInvalid
  // survive the continueAsNew handoff.
  await updateTokenInfos(tokenInfos)

  if (deferred) {
    // All tokens rate-limited or invalid and repos still pending. Calling continueAsNew here
    // would spin: findObsoleteRepos returns the same repos, they defer again, etc.
    // Return instead — the daily schedule (scheduleCheckReposWithObsoleteSecurityInsights)
    // will re-trigger the workflow; rate-limit timestamps persist in Redis so recovery is
    // resumable once the 1h window elapses.
    return
  }

  await continueAsNew<typeof triggerSecurityInsightsCheckForRepos>({
    failedRepoUrls,
  })
}

function refreshExpiredRateLimits(tokenInfos: ITokenInfo[], nowMs: number): void {
  for (const t of tokenInfos) {
    if (
      t.isRateLimited &&
      t.rateLimitedAt &&
      nowMs - new Date(t.rateLimitedAt).getTime() > ONE_HOUR_MS
    ) {
      t.isRateLimited = false
      t.rateLimitedAt = undefined
    }
  }
}

function getNextToken(tokenInfos: ITokenInfo[], nowMs: number): ITokenInfo | null {
  refreshExpiredRateLimits(tokenInfos, nowMs)

  const usableTokenInfos = tokenInfos.filter((t) => !t.inUse && !t.isRateLimited && !t.isInvalid)

  // sort usable tokens by last used date from oldest to newest
  const sortedTokenInfos = usableTokenInfos.sort((a, b) => {
    const aTime = new Date(a.lastUsed).getTime()
    const bTime = new Date(b.lastUsed).getTime()
    return aTime - bTime
  })

  return sortedTokenInfos[0] ?? null
}

function unwrapApplicationFailure(error: unknown): ApplicationFailure | null {
  let e: unknown = error
  while (e) {
    if (e instanceof ApplicationFailure) return e
    e = (e as { cause?: unknown }).cause
  }
  return null
}

async function releaseToken(tokenInfos: ITokenInfo[], token: string): Promise<void> {
  const tokenInfo = tokenInfos.find((tokenInfo) => tokenInfo.token === token)
  if (tokenInfo) {
    tokenInfo.inUse = false
    tokenInfo.lastUsed = new Date()
  }
}

async function acquireToken(tokenInfos: ITokenInfo[], token: string): Promise<void> {
  const tokenInfo = tokenInfos.find((tokenInfo) => tokenInfo.token === token)
  if (tokenInfo) {
    tokenInfo.inUse = true
  }
}
