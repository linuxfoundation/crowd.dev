import { ApplicationFailure } from '@temporalio/client'

import { getGithubInstallationToken } from '@crowd/common_services'
import {
  findReposForStarSnapshot as findReposForStarSnapshotQx,
  findReposNeedingStarBackfill as findReposNeedingStarBackfillQx,
  recordStarBackfillFailure,
  recordStarBackfillSuccess,
  upsertStarSnapshot,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'
import { IRepoForStarSnapshot } from '@crowd/types'

import {
  CoreRateLimiter,
  RepoBackfillResult,
  SECONDARY_RATE_LIMIT_COOLDOWN_MS,
  backfillRepo,
  createCoreRateLimiter,
} from '../backfill/starSnapshotBackfill'
import { parseGithubRepoUrl } from '../githubRepoUrl'
import { svc } from '../main'

const SELF_HEAL_RESERVED_CORE_RATE_LIMIT = 2_000
const SELF_HEAL_DEAD_LETTER_AFTER = 3
// Covers a batch's full processing time (incl. rate-limit backoffs) while still auto-releasing
// a crashed/stuck claim before the next daily schedule tick could double-dispatch it.
const SELF_HEAL_INFLIGHT_TTL_SECONDS = 6 * 60 * 60

let selfHealRateLimiter: CoreRateLimiter | undefined
// Lazy (svc.log isn't ready at module load) singleton per worker process - the reserved
// floor is a real GitHub quota shared across every concurrent activity call, not per-call.
function getSelfHealRateLimiter(): CoreRateLimiter {
  if (!selfHealRateLimiter) {
    selfHealRateLimiter = createCoreRateLimiter(SELF_HEAL_RESERVED_CORE_RATE_LIMIT, svc.log)
  }
  return selfHealRateLimiter
}

let selfHealInflightCache: RedisCache | undefined
function getSelfHealInflightCache(): RedisCache {
  if (!selfHealInflightCache) {
    selfHealInflightCache = new RedisCache('starBackfillInflight', svc.redis, svc.log)
  }
  return selfHealInflightCache
}

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const FETCH_TIMEOUT_MS = 30_000
const MAX_ALIAS_ATTEMPTS = 3
const ALIAS_RETRY_DELAY_MS = 2_000

const NON_RETRYABLE_GRAPHQL_ERROR_TYPES = new Set(['NOT_FOUND', 'FORBIDDEN', 'INSUFFICIENT_SCOPES'])

interface GraphqlAliasError {
  type?: string
  message?: string
}

interface BatchGraphqlResponse {
  data?: Record<string, { stargazerCount: number } | null>
  errors?: Array<GraphqlAliasError & { path?: Array<string | number> }>
}

function isRetryableAliasError(error?: GraphqlAliasError): boolean {
  if (error?.message?.toLowerCase().includes('rate limit')) {
    return true
  }
  return !error?.type || !NON_RETRYABLE_GRAPHQL_ERROR_TYPES.has(error.type)
}

export interface RepoStarFetchResult {
  repositoryId: string
  repoUrl: string
  starCount?: number
  error?: string
}

function buildBatchQuery(repos: Array<{ owner: string; name: string }>): {
  query: string
  variables: Record<string, string>
} {
  const variables: Record<string, string> = {}
  const varDefs: string[] = []
  const fields: string[] = []

  repos.forEach((repo, i) => {
    variables[`owner${i}`] = repo.owner
    variables[`name${i}`] = repo.name
    varDefs.push(`$owner${i}: String!, $name${i}: String!`)
    fields.push(`r${i}: repository(owner: $owner${i}, name: $name${i}) { stargazerCount }`)
  })

  return {
    query: `query(${varDefs.join(', ')}) { ${fields.join(' ')} }`,
    variables,
  }
}

async function queryStargazerCounts(
  entries: Array<{ repo: IRepoForStarSnapshot; owner: string; name: string }>,
): Promise<BatchGraphqlResponse> {
  const { query, variables } = buildBatchQuery(entries)
  const token = await getGithubInstallationToken()

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  try {
    const response = await fetch(GITHUB_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        Authorization: `bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query, variables }),
      signal: controller.signal,
    })

    if (response.status === 401) {
      throw new Error('GitHub auth failure (401) fetching stargazer counts')
    }

    if (response.status === 403) {
      const body = await response.text()
      const bodyLower = body.toLowerCase()
      if (bodyLower.includes('rate limit')) {
        throw new Error('GitHub rate limit hit fetching stargazer counts')
      }
      if (bodyLower.includes('ip allow list')) {
        // Org IP allow list blocks the whole batch at the HTTP level before any field resolves,
        // so which repo caused it can't be told apart - synthesize per-alias errors instead.
        return {
          errors: entries.map((entry, i) => ({
            path: [`r${i}`],
            type: 'FORBIDDEN',
            message: `org IP allow list may be blocking one of this batch's repos (incl. ${entry.owner}/${entry.name})`,
          })),
        }
      }
      throw ApplicationFailure.nonRetryable(
        'GitHub auth failure (403) fetching stargazer counts',
        'AUTH_ERROR',
      )
    }

    if (!response.ok) {
      throw new Error(`GitHub API error ${response.status} fetching stargazer counts`)
    }

    return (await response.json()) as BatchGraphqlResponse
  } finally {
    clearTimeout(timeoutId)
  }
}

// Retries only failed aliases, up to MAX_ALIAS_ATTEMPTS, before rejecting; a query rejects
// only on the first attempt, before any alias in this call has been fetched or persisted.
async function fetchStargazerCounts(
  entries: Array<{ repo: IRepoForStarSnapshot; owner: string; name: string }>,
): Promise<RepoStarFetchResult[]> {
  const results: RepoStarFetchResult[] = []
  let pending = entries

  for (let attempt = 1; attempt <= MAX_ALIAS_ATTEMPTS && pending.length > 0; attempt++) {
    let json: BatchGraphqlResponse
    try {
      json = await queryStargazerCounts(pending)
    } catch (error) {
      if (attempt === 1) {
        throw error
      }
      if (attempt < MAX_ALIAS_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, ALIAS_RETRY_DELAY_MS * attempt))
        continue
      }
      for (const entry of pending) {
        results.push({
          repositoryId: entry.repo.repositoryId,
          repoUrl: entry.repo.repoUrl,
          error: (error as Error).message,
        })
      }
      pending = []
      break
    }

    const topLevelError = json.errors?.find((error) => !error.path?.length)
    if (topLevelError) {
      const message = `GraphQL error fetching stargazer counts: ${topLevelError.message ?? 'unknown error'}`
      if (attempt === 1) {
        throw new Error(message)
      }
      if (attempt < MAX_ALIAS_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, ALIAS_RETRY_DELAY_MS * attempt))
        continue
      }
      for (const entry of pending) {
        results.push({
          repositoryId: entry.repo.repositoryId,
          repoUrl: entry.repo.repoUrl,
          error: message,
        })
      }
      pending = []
      break
    }

    const errorsByAlias = new Map<string, GraphqlAliasError>()
    for (const error of json.errors ?? []) {
      const alias = error.path?.[0]
      if (typeof alias === 'string') {
        errorsByAlias.set(alias, error)
      }
    }

    const stillPending: typeof pending = []

    pending.forEach((entry, i) => {
      const alias = `r${i}`
      const starCount = json.data?.[alias]?.stargazerCount

      if (starCount !== undefined && starCount !== null) {
        results.push({
          repositoryId: entry.repo.repositoryId,
          repoUrl: entry.repo.repoUrl,
          starCount,
        })
        return
      }

      const error = errorsByAlias.get(alias)
      const message = error?.message ?? 'No repository data returned'

      if (attempt < MAX_ALIAS_ATTEMPTS && isRetryableAliasError(error)) {
        stillPending.push(entry)
        return
      }

      results.push({
        repositoryId: entry.repo.repositoryId,
        repoUrl: entry.repo.repoUrl,
        error: message,
      })
    })

    pending = stillPending
    if (pending.length > 0) {
      await new Promise((resolve) => setTimeout(resolve, ALIAS_RETRY_DELAY_MS * attempt))
    }
  }

  return results
}

export async function fetchAndSaveStarSnapshotBatch(
  repos: IRepoForStarSnapshot[],
  capturedAt: string,
): Promise<RepoStarFetchResult[]> {
  const parsed: Array<{ repo: IRepoForStarSnapshot; owner: string; name: string }> = []
  const unparseableResults: RepoStarFetchResult[] = []

  for (const repo of repos) {
    try {
      const { owner, name } = parseGithubRepoUrl(repo.repoUrl)
      parsed.push({ repo, owner, name })
    } catch (error) {
      unparseableResults.push({
        repositoryId: repo.repositoryId,
        repoUrl: repo.repoUrl,
        error: (error as Error).message,
      })
    }
  }

  if (parsed.length === 0) {
    return unparseableResults
  }

  const results = await fetchStargazerCounts(parsed)

  const qx = pgpQx(svc.postgres.writer.connection())
  for (const result of results) {
    if (result.starCount !== undefined) {
      await upsertStarSnapshot(qx, result.repositoryId, result.starCount, capturedAt)
    }
  }

  return [...results, ...unparseableResults]
}

export async function findReposForStarSnapshot(
  limit?: number,
  afterUrl?: string,
): Promise<IRepoForStarSnapshot[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findReposForStarSnapshotQx(qx, limit, afterUrl)
}

export async function findReposNeedingStarBackfill(
  limit?: number,
  afterUrl?: string,
): Promise<IRepoForStarSnapshot[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findReposNeedingStarBackfillQx(qx, limit, afterUrl)
}

// A leftover claim only costs a repo one skipped run before the TTL clears it - not worth
// forcing a full activity retry (re-fetching the entire stargazer history) over.
async function releaseInflightClaim(cache: RedisCache, repositoryId: string): Promise<void> {
  try {
    await cache.delete(repositoryId)
  } catch (err) {
    svc.log.warn(
      { repositoryId, error: (err as Error)?.message ?? err },
      'failed to release star backfill in-flight claim, will auto-expire via TTL',
    )
  }
}

export type BackfillRepoStarHistoryResult =
  | { outcome: 'rate-limited'; waitMs: number }
  | { outcome: 'in-flight' }
  | { outcome: 'done' }

export async function backfillRepoStarHistory(
  repo: IRepoForStarSnapshot,
  ownerId: string,
): Promise<BackfillRepoStarHistoryResult> {
  const inflightCache = getSelfHealInflightCache()
  // Claims the repo for this batch (ownerId) so a different day's batch backs off instead of
  // re-processing it; re-claiming with the same ownerId is a no-op that confirms it.
  const holder = await inflightCache.setIfNotExistsOrGet(
    repo.repositoryId,
    ownerId,
    SELF_HEAL_INFLIGHT_TTL_SECONDS,
  )
  if (holder !== ownerId) {
    return { outcome: 'in-flight' }
  }
  // SETNX only sets the TTL on the first claim - renew it every attempt too, or a repo
  // that bounces through backoffs longer than the TTL loses its claim mid-processing.
  await inflightCache.set(repo.repositoryId, ownerId, SELF_HEAL_INFLIGHT_TTL_SECONDS)

  const rateLimiter = getSelfHealRateLimiter()
  const waitMs = rateLimiter.peekWaitMs()
  if (waitMs > 0) {
    return { outcome: 'rate-limited', waitMs }
  }

  const qx = pgpQx(svc.postgres.writer.connection())
  let result: RepoBackfillResult
  try {
    result = await backfillRepo(qx, repo, rateLimiter, svc.log, { dryRun: false, failFast: true })
  } catch (err) {
    const message = (err as Error)?.message ?? String(err)
    if (message.toLowerCase().includes('rate limit')) {
      return {
        outcome: 'rate-limited',
        waitMs: rateLimiter.peekWaitMs() || SECONDARY_RATE_LIMIT_COOLDOWN_MS,
      }
    }
    try {
      await recordStarBackfillFailure(
        qx,
        repo.repositoryId,
        (err as Error)?.name ?? 'Error',
        message,
        SELF_HEAL_DEAD_LETTER_AFTER,
      )
    } catch (recordErr) {
      // Failure is already known here - don't let a transient marker-write error also fail the
      // activity and force a full retry (re-fetching all stargazer history) just to log it.
      svc.log.warn(
        { repositoryId: repo.repositoryId, error: (recordErr as Error)?.message ?? recordErr },
        'failed to record star backfill failure marker, will retry on next self-heal run',
      )
    }
    await releaseInflightClaim(inflightCache, repo.repositoryId)
    return { outcome: 'done' }
  }

  if (result.status === 'skipped-negative-count') {
    // Deterministic for this repo's actual GitHub data - retrying it plain would refetch its
    // full history every run forever, so it's dead-lettered like any other failure.
    try {
      await recordStarBackfillFailure(
        qx,
        repo.repositoryId,
        'NegativeStarCountAnomaly',
        'backward-anchored reconstruction produced a negative star count',
        SELF_HEAL_DEAD_LETTER_AFTER,
      )
    } catch (err) {
      svc.log.warn(
        { repositoryId: repo.repositoryId, error: (err as Error)?.message ?? err },
        'failed to record star backfill anomaly marker, will retry on next self-heal run',
      )
    }
    await releaseInflightClaim(inflightCache, repo.repositoryId)
    return { outcome: 'done' }
  }

  try {
    await recordStarBackfillSuccess(qx, repo.repositoryId)
  } catch (err) {
    // Fetch and row writes already succeeded - don't let a transient marker-write error force
    // a full retry (re-fetching all stargazer history); it just stays a candidate till next run.
    svc.log.warn(
      { repositoryId: repo.repositoryId, error: (err as Error)?.message ?? err },
      'failed to record star backfill completion marker, will retry on next self-heal run',
    )
  }
  await releaseInflightClaim(inflightCache, repo.repositoryId)
  return { outcome: 'done' }
}
