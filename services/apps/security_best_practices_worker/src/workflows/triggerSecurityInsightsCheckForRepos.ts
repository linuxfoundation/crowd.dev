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

const { findObsoleteRepos, initializeTokenInfos, updateTokenInfos, getCurrentTimeMs } =
  proxyActivities<typeof activities>({
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

  const info = workflowInfo()
  const failedRepoUrls = args?.failedRepoUrls || []

  // Wall-clock time comes from the getCurrentTimeMs activity — workflow code can't call
  // Date.now() (Temporal determinism rule). Refreshed each outer-loop iteration so rate-limit
  // cooldowns can expire mid-run instead of only at continueAsNew boundaries.
  let currentTimeMs = await getCurrentTimeMs()

  // Token state is persisted in Redis via updateTokenInfos so isRateLimited (with rateLimitedAt
  // for expiry) and isInvalid survive across continueAsNew batches.
  const tokenInfos: ITokenInfo[] = await initializeTokenInfos()

  // Empty pool means CROWD_GITHUB_PERSONAL_ACCESS_TOKENS is misconfigured. Without this guard
  // an empty array would satisfy `tokenInfos.every(t => t.isInvalid)` and silently mark every
  // obsolete repo as failed for the continueAsNew chain, hiding the config error.
  if (tokenInfos.length === 0) {
    throw ApplicationFailure.create({
      message: 'No GitHub tokens configured (CROWD_GITHUB_PERSONAL_ACCESS_TOKENS is empty)',
      type: 'TokenPoolEmpty',
      nonRetryable: true,
    })
  }

  // Scale attempts to pool size so a repo isn't marked failed while untried tokens remain
  // (e.g. 6 PATs where the first 5 attempts all 403 on different tokens).
  const MAX_TOKEN_ATTEMPTS = Math.max(5, tokenInfos.length)

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
      // Refresh wall-clock time on retries only. First attempt intentionally reuses the
      // outer-loop's currentTimeMs so processRepo runs synchronously up to
      // `tokenInfo.inUse = true` — otherwise the outer loop can oversubscribe
      // MAX_PARALLEL_CHILDREN tasks against a smaller token pool before any inUse mark lands.
      if (attempts > 0) {
        currentTimeMs = await getCurrentTimeMs()
      }
      const tokenInfo = getNextToken(tokenInfos, currentTimeMs)
      if (!tokenInfo) {
        // No usable token — requeue if any may recover (rate-limited tokens expire after 1h
        // or free up from concurrent children); fail if all are permanently invalid.
        const allPermanentlyInvalid = tokenInfos.every((t) => t.isInvalid)
        if (allPermanentlyInvalid) {
          console.error(`No usable tokens for repo ${repo.repoUrl}, skipping`)
          failedRepoUrls.push(repo.repoUrl)
        } else {
          queue.unshift(repo)
        }
        break
      }
      const token = tokenInfo.token
      tokenInfo.inUse = true

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
        // ApplicationFailure — traverse the cause chain to find the root ApplicationFailure.
        const appFailure = unwrapApplicationFailure(error)
        if (appFailure?.type === 'Token403Error') {
          tokenInfo.isRateLimited = true
          tokenInfo.rateLimitedAt =
            extractFailureTimestamp(appFailure) ?? new Date(currentTimeMs).toISOString()
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
          failedRepoUrls.push(repo.repoUrl)
          break
        }
      } finally {
        tokenInfo.inUse = false
        tokenInfo.lastUsed = new Date(currentTimeMs)
      }
    }

    if (attempts >= MAX_TOKEN_ATTEMPTS) {
      const allPermanentlyInvalid = tokenInfos.every((t) => t.isInvalid)
      if (!allPermanentlyInvalid) {
        console.warn(`Exhausted token attempts for repo ${repo.repoUrl}, requeuing`)
        queue.unshift(repo)
      } else {
        console.error(`All tokens invalid for repo ${repo.repoUrl}, skipping`)
        failedRepoUrls.push(repo.repoUrl)
      }
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
    // Refresh wall-clock time each iteration so mid-run rate-limit cooldowns can expire
    // without waiting for the next continueAsNew batch.
    currentTimeMs = await getCurrentTimeMs()
    while (queue.length > 0 && activeTasks.length < MAX_PARALLEL_CHILDREN) {
      // Only start a task if a token is currently free — avoids false "no token" failures
      // when all tokens are temporarily held by other concurrent tasks.
      refreshExpiredRateLimits(tokenInfos, currentTimeMs)
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
      // No active tasks and no free tokens. If every token is permanently invalid, deferring
      // would loop forever across scheduled runs — fail the remaining repos so ops sees the
      // problem instead of a silent stall.
      const allPermanentlyInvalid = tokenInfos.every((t) => t.isInvalid)
      if (allPermanentlyInvalid) {
        console.error(
          `All tokens permanently invalid; marking ${queue.length} remaining repos as failed`,
        )
        for (const repo of queue) failedRepoUrls.push(repo.repoUrl)
        queue.length = 0
        break
      }
      // Otherwise defer: remaining repos are still obsolete and findObsoleteRepos will pick
      // them up on the next scheduled run (daily cron `0 8 * * *`). We don't `sleep()` here
      // to the earliest cooldown because the 30-day obsolescence window makes a ~23h delay
      // negligible, and holding a worker slot idle for hours per rate-limit event isn't worth
      // the cost. `rateLimitedAt` persists in Redis so the 1h expiry still applies on resume.
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
    if (!t.isRateLimited) continue
    // Clear rate-limit when the timestamp is missing, unparseable, or older than 1h.
    // Guarding against NaN avoids permanently wedging a token due to malformed cache data.
    const rateMs = t.rateLimitedAt ? new Date(t.rateLimitedAt).getTime() : NaN
    if (!Number.isFinite(rateMs) || nowMs - rateMs > ONE_HOUR_MS) {
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

// Activity records the wall-clock failure time via ApplicationFailure.details[0] (ms).
function extractFailureTimestamp(appFailure: ApplicationFailure): string | null {
  const details = appFailure.details as unknown[] | undefined
  const ts = details?.[0]
  if (typeof ts !== 'number' || !Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}
