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

export async function triggerSecurityInsightsCheckForRepos(
  args: ITriggerSecurityInsightsCheckForReposParams,
): Promise<void> {
  const REPOS_OBSOLETE_AFTER_SECONDS = 30 * 24 * 60 * 60 // 30 days
  const LIMIT_REPOS_TO_CHECK_PER_RUN = 100
  const MAX_PARALLEL_CHILDREN = 5
  const MAX_TOKEN_ATTEMPTS = 5

  const info = workflowInfo()
  const failedRepoUrls = args?.failedRepoUrls || []

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
      const tokenInfo = getNextToken(tokenInfos)
      if (!tokenInfo) {
        console.error(`No usable tokens for repo ${repo.repoUrl}, skipping`)
        failedRepoUrls.push(repo.repoUrl)
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
          attempts++
          continue // retry with a different token
        } else if (appFailure?.type === 'TokenAuthError') {
          console.error(
            `Token invalid/expired for repo ${repo.repoUrl}, excluding token for this run`,
          )
          tokenInfo.isRateLimited = true
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
  }

  /**
   * We fire up to MAX_PARALLEL_CHILDREN concurrent tasks to process the repos.
   * Each task will acquire a token, process the repo, and then release the token.
   * If a task fails with a Token403Error, we will retry it with a different token.
   * When a task finishes (checked through Promise.race),
   * it will be removed from the activeTasks array and the next task will be started.
   * This way we don't need to wait for all tasks to finish before starting new ones.
   */
  while (queue.length > 0 || activeTasks.length > 0) {
    while (queue.length > 0 && activeTasks.length < MAX_PARALLEL_CHILDREN) {
      const repo = queue.shift()
      const task = processRepo(repo).finally(() => {
        const index = activeTasks.indexOf(task)
        if (index >= 0) activeTasks.splice(index, 1)
      })
      activeTasks.push(task)
    }

    // Wait for any one to finish before refilling
    if (activeTasks.length > 0) {
      await Promise.race(activeTasks)
    }
  }

  await updateTokenInfos(tokenInfos)

  await continueAsNew<typeof triggerSecurityInsightsCheckForRepos>({
    failedRepoUrls,
  })
}

function getNextToken(tokenInfos: ITokenInfo[]): ITokenInfo | null {
  const usableTokenInfos = tokenInfos.filter((token) => !token.inUse && !token.isRateLimited)

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
