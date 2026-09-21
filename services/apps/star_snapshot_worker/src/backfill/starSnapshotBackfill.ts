import { getGithubInstallationToken } from '@crowd/common_services'
import {
  findReposForStarSnapshot,
  findStarSnapshotsForRepos,
  upsertStarSnapshot,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { Logger } from '@crowd/logging'
import { IRepoForStarSnapshot } from '@crowd/types'

import { parseGithubRepoUrl } from '../githubRepoUrl'

const GITHUB_API_VERSION = '2022-11-28'
const FETCH_TIMEOUT_MS = 30_000
const WEEKS_PER_PAGE = 30
const REPO_PAGE_SIZE = 500
// The current-count fetch happens a moment after the last history page, so a
// star/unstar landing in between would show up as drift here.
const RECONCILIATION_TOLERANCE = 2

interface StargazerHistoryWeek {
  week: number
  total: number
  days: number[]
}

interface DailyDelta {
  date: string
  delta: number
}

interface DailyCount {
  date: string
  count: number
}

export interface StarSnapshotBackfillOptions {
  reservedCoreRateLimit: number
  concurrency: number
  dryRun: boolean
  afterUrl?: string
  // Repos already backfilled in a previous run - skipped without hitting GitHub;
  // mutated in place here and persisted by the caller for the next run.
  completedRepoIds?: Set<string>
  isShuttingDown: () => boolean
  onProgress?: (afterUrl: string, totals: StarSnapshotBackfillTotals) => Promise<void> | void
}

export interface StarSnapshotBackfillTotals {
  reposProcessed: number
  reposSkippedAlreadyBackfilled: number
  reposSkippedNoHistory: number
  reposSkippedNegativeCount: number
  reposReconciled: number
  reposAnchoredBackward: number
  reposFailed: number
  reposRecoveredOnRetry: number
  daysWritten: number
  // false when the run stopped early (shutdown signal) rather than exhausting all repos —
  // callers use this to decide whether a resume checkpoint should be kept or cleared.
  completed: boolean
}

// Reading `rel="last"` off the first page's Link header gets the exact page count up
// front, saving the one wasted trailing empty-page call per repo.
function parseLastPage(linkHeader: string | null): number | undefined {
  if (!linkHeader) {
    return undefined
  }
  const match = linkHeader.match(/[?&]page=(\d+)[^>]*>;\s*rel="last"/)
  return match ? Number(match[1]) : undefined
}

export const SECONDARY_RATE_LIMIT_COOLDOWN_MS = 60_000

export function createCoreRateLimiter(reservedFloor: number, log: Logger) {
  let remaining = Infinity
  let resetAtMs = 0
  let secondaryCooldownUntilMs = 0

  return {
    observe(headers: Headers): void {
      // headers.get() returns null when absent, and Number(null) is 0 - checking presence
      // explicitly keeps a response with no rate-limit headers from being read as "zero quota".
      const remainingHeader = headers.get('x-ratelimit-remaining')
      const resetHeader = headers.get('x-ratelimit-reset')
      const observedRemaining = remainingHeader !== null ? Number(remainingHeader) : undefined
      const observedReset = resetHeader !== null ? Number(resetHeader) : undefined
      const observedResetMs =
        observedReset !== undefined && Number.isFinite(observedReset)
          ? observedReset * 1000
          : undefined

      // A response from an already-passed reset window carries no information about
      // the current one - ignore it entirely rather than let it rewind resetAtMs.
      if (observedResetMs !== undefined && observedResetMs < resetAtMs) {
        return
      }

      if (observedRemaining !== undefined && Number.isFinite(observedRemaining)) {
        remaining =
          observedResetMs !== undefined && observedResetMs > resetAtMs
            ? observedRemaining
            : Math.min(remaining, observedRemaining)
      }
      if (observedResetMs !== undefined) {
        resetAtMs = observedResetMs
      }
    },

    // GitHub's secondary/abuse limit only surfaces as a 403/429, never in headers -
    // callers report it here so every in-flight call backs off, not just this one.
    noteSecondaryRateLimit(retryAfterMs?: number): void {
      secondaryCooldownUntilMs = Math.max(
        secondaryCooldownUntilMs,
        Date.now() + (retryAfterMs ?? SECONDARY_RATE_LIMIT_COOLDOWN_MS),
      )
    },

    async throttleIfNeeded(): Promise<void> {
      // Re-check the shared deadline after each wait - another in-flight call can push
      // secondaryCooldownUntilMs further out while this one sleeps.
      while (secondaryCooldownUntilMs > Date.now()) {
        const secondaryWaitMs = secondaryCooldownUntilMs - Date.now()
        log.warn(
          { waitMs: secondaryWaitMs },
          'GitHub secondary rate limit cooldown in effect, backing off',
        )
        await new Promise((resolve) => setTimeout(resolve, secondaryWaitMs))
      }

      if (remaining > reservedFloor) {
        // No await before this line, so concurrent callers see the decrement
        // before any of them observes the real value off a response.
        remaining--
        return
      }
      const waitMs = Math.max(resetAtMs - Date.now(), 0) + 1_000
      log.warn(
        { remaining, reservedFloor, waitMs },
        'GitHub core rate limit near reserved floor, backing off until reset',
      )
      await new Promise((resolve) => setTimeout(resolve, waitMs))
    },

    // Non-blocking version of throttleIfNeeded, for a caller (e.g. a Temporal activity) that
    // can't hold its startToCloseTimeout open for the wait - it backs off durably instead.
    peekWaitMs(): number {
      if (secondaryCooldownUntilMs > Date.now()) {
        return secondaryCooldownUntilMs - Date.now()
      }
      // A failFast caller may never make a real call after this (reserveOrThrow throws instead),
      // so treat a passed reset as fresh (remaining = Infinity) or it'd stay stuck at the floor.
      if (resetAtMs > 0 && Date.now() >= resetAtMs) {
        remaining = Infinity
      }
      if (remaining > reservedFloor) {
        return 0
      }
      return Math.max(resetAtMs - Date.now(), 0) + 1_000
    },

    // Same reservation as throttleIfNeeded but synchronous - throws so a caller that peeked
    // once at entry can't have quota run out from under it before its own GitHub call.
    reserveOrThrow(): void {
      if (secondaryCooldownUntilMs > Date.now()) {
        throw new Error('GitHub rate limit hit: secondary cooldown in effect')
      }
      // See peekWaitMs - without this, once remaining drops to the floor it never
      // recovers, since throwing here means no real call ever runs to refresh it via observe().
      if (resetAtMs > 0 && Date.now() >= resetAtMs) {
        remaining = Infinity
      }
      if (remaining > reservedFloor) {
        remaining--
        return
      }
      throw new Error('GitHub rate limit hit: core rate limit near reserved floor')
    },
  }
}

export type CoreRateLimiter = ReturnType<typeof createCoreRateLimiter>

// failFast=true throws synchronously instead of awaiting the wait, for a caller (a Temporal
// activity) that can't hold its startToCloseTimeout open for GitHub's reset.
async function acquireRateLimitSlot(
  rateLimiter: CoreRateLimiter,
  failFast: boolean,
): Promise<void> {
  if (failFast) {
    rateLimiter.reserveOrThrow()
    return
  }
  await rateLimiter.throttleIfNeeded()
}

async function githubGet(url: string, token: string): Promise<Response> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)
  try {
    return await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': GITHUB_API_VERSION,
      },
      signal: controller.signal,
    })
  } finally {
    clearTimeout(timeoutId)
  }
}

async function assertOk(
  response: Response,
  owner: string,
  name: string,
  what: string,
  rateLimiter: CoreRateLimiter,
): Promise<void> {
  if (response.ok) {
    return
  }
  if (response.status === 401) {
    throw new Error(`GitHub auth failure (401) fetching ${what} for ${owner}/${name}`)
  }
  if (response.status === 404) {
    throw new Error(`Repo not found (404) fetching ${what} for ${owner}/${name}`)
  }
  if (response.status === 403 || response.status === 429) {
    const retryAfterHeader = response.headers.get('retry-after')
    const retryAfterSeconds = retryAfterHeader === null ? NaN : Number(retryAfterHeader)
    const retryAfterMs = Number.isFinite(retryAfterSeconds) ? retryAfterSeconds * 1000 : undefined

    if (response.status === 429) {
      rateLimiter.noteSecondaryRateLimit(retryAfterMs)
      throw new Error(`GitHub rate limit hit (429) fetching ${what} for ${owner}/${name}`)
    }

    const body = await response.text()
    const bodyLower = body.toLowerCase()
    // Only a retry-after header or explicit secondary/abuse wording signals GitHub's secondary
    // limit; misclassifying a primary 403 here would use the short cooldown, not the real reset.
    const isSecondaryRateLimit =
      retryAfterMs !== undefined ||
      bodyLower.includes('secondary rate limit') ||
      bodyLower.includes('abuse detection')
    if (isSecondaryRateLimit) {
      rateLimiter.noteSecondaryRateLimit(retryAfterMs)
      throw new Error(`GitHub secondary rate limit hit fetching ${what} for ${owner}/${name}`)
    }
    if (bodyLower.includes('rate limit')) {
      throw new Error(`GitHub rate limit hit fetching ${what} for ${owner}/${name}`)
    }
    throw new Error(`GitHub auth failure (403) fetching ${what} for ${owner}/${name}`)
  }
  throw new Error(`GitHub API error ${response.status} fetching ${what} for ${owner}/${name}`)
}

async function fetchStargazerHistory(
  owner: string,
  name: string,
  token: string,
  rateLimiter: CoreRateLimiter,
  failFast: boolean,
): Promise<StargazerHistoryWeek[]> {
  const baseUrl = `https://api.github.com/repos/${owner}/${name}/stargazers/history?per_page=${WEEKS_PER_PAGE}`

  await acquireRateLimitSlot(rateLimiter, failFast)
  const firstResponse = await githubGet(`${baseUrl}&page=1`, token)
  rateLimiter.observe(firstResponse.headers)
  await assertOk(firstResponse, owner, name, 'stargazer history', rateLimiter)

  const firstPage = (await firstResponse.json()) as StargazerHistoryWeek[]
  if (firstPage.length === 0) {
    return []
  }

  const lastPage = parseLastPage(firstResponse.headers.get('link')) ?? 1
  const weeks = [...firstPage]

  for (let page = 2; page <= lastPage; page++) {
    await acquireRateLimitSlot(rateLimiter, failFast)
    const response = await githubGet(`${baseUrl}&page=${page}`, token)
    rateLimiter.observe(response.headers)
    await assertOk(response, owner, name, 'stargazer history', rateLimiter)
    weeks.push(...((await response.json()) as StargazerHistoryWeek[]))
  }

  return weeks
}

async function fetchCurrentStarCount(
  owner: string,
  name: string,
  token: string,
  rateLimiter: CoreRateLimiter,
  failFast: boolean,
): Promise<number> {
  await acquireRateLimitSlot(rateLimiter, failFast)
  const response = await githubGet(`https://api.github.com/repos/${owner}/${name}`, token)
  rateLimiter.observe(response.headers)
  await assertOk(response, owner, name, 'current star count', rateLimiter)
  const body = (await response.json()) as { stargazers_count: number }
  return body.stargazers_count
}

// `week` is the Unix timestamp (seconds) of that week's Sunday; `days[0..6]` are the
// Sun-Sat per-day deltas GitHub attributes to it.
function buildDailyDeltas(weeks: StargazerHistoryWeek[]): DailyDelta[] {
  const sorted = [...weeks].sort((a, b) => a.week - b.week)
  const daily: DailyDelta[] = []
  for (const week of sorted) {
    for (let day = 0; day < 7; day++) {
      const date = new Date((week.week + day * 86_400) * 1000)
      daily.push({ date: date.toISOString().slice(0, 10), delta: week.days[day] ?? 0 })
    }
  }
  return daily
}

function buildForwardCounts(daily: DailyDelta[]): DailyCount[] {
  let running = 0
  return daily.map(({ date, delta }) => {
    running += delta
    return { date, count: running }
  })
}

// Fallback when forward-summed deltas don't reconcile: anchor the most recent day to
// the known-good total and walk backward, undoing each day's delta.
function buildBackwardCounts(daily: DailyDelta[], knownCurrentTotal: number): DailyCount[] {
  const counts = Array.from({ length: daily.length }) as number[]
  let running = knownCurrentTotal
  for (let i = daily.length - 1; i >= 0; i--) {
    counts[i] = running
    running -= daily[i].delta
  }
  return daily.map((d, i) => ({ date: d.date, count: counts[i] }))
}

export interface RepoBackfillResult {
  status: 'reconciled' | 'anchored' | 'skipped-no-history' | 'skipped-negative-count'
  daysWritten: number
}

export interface BackfillRepoOptions {
  dryRun: boolean
  // true for callers that can't block on GitHub's rate-limit reset (e.g. a Temporal activity
  // with a short startToCloseTimeout) - fails fast with a "rate limit" error instead.
  failFast: boolean
}

export async function backfillRepo(
  qx: QueryExecutor,
  repo: IRepoForStarSnapshot,
  rateLimiter: CoreRateLimiter,
  log: Logger,
  options: BackfillRepoOptions,
): Promise<RepoBackfillResult> {
  const { owner, name } = parseGithubRepoUrl(repo.repoUrl)
  const token = await getGithubInstallationToken()

  const weeks = await fetchStargazerHistory(owner, name, token, rateLimiter, options.failFast)
  if (weeks.length === 0) {
    return { status: 'skipped-no-history', daysWritten: 0 }
  }

  const daily = buildDailyDeltas(weeks)
  const forward = buildForwardCounts(daily)
  const knownCurrentTotal = await fetchCurrentStarCount(
    owner,
    name,
    token,
    rateLimiter,
    options.failFast,
  )

  const reconciled =
    Math.abs(forward[forward.length - 1].count - knownCurrentTotal) <= RECONCILIATION_TOLERANCE
  if (!reconciled) {
    log.warn(
      { repoUrl: repo.repoUrl, forwardTotal: forward[forward.length - 1].count, knownCurrentTotal },
      'stargazer history did not reconcile with current star count, anchoring backward from known total instead',
    )
  }
  const allRows = reconciled ? forward : buildBackwardCounts(daily, knownCurrentTotal)

  if (allRows.some((row) => row.count < 0)) {
    // Backward anchoring can undershoot below zero when the history-derived deltas
    // overshoot the known total by more than reconciliation allows for.
    log.warn(
      { repoUrl: repo.repoUrl, knownCurrentTotal },
      'backward-anchored reconstruction produced a negative star count, skipping repo',
    )
    return { status: 'skipped-negative-count', daysWritten: 0 }
  }

  // Never touch "today" (CM-1438 owns it) - diff the rest against the DB so any missing
  // day, not just ones behind the earliest snapshot, gets filled.
  const today = new Date().toISOString().slice(0, 10)
  const candidateRows = allRows.filter((row) => row.date < today)

  const existingRows =
    candidateRows.length > 0
      ? await findStarSnapshotsForRepos(qx, [repo.repositoryId], {
          from: `${candidateRows[0].date}T00:00:00.000Z`,
          // End of day, not midnight - CM-1438 stores "today" at wall-clock time, not T00:00:00.
          to: `${candidateRows[candidateRows.length - 1].date}T23:59:59.999Z`,
        })
      : []
  // capturedAt is pg's raw text output (session-timezone dependent) - reparse as UTC so it
  // lines up with GitHub's UTC-based dates instead of drifting a day near local midnight.
  const existingDates = new Set(
    existingRows.map((row) => new Date(row.capturedAt).toISOString().slice(0, 10)),
  )
  const rowsToWrite = candidateRows.filter((row) => !existingDates.has(row.date))

  if (!options.dryRun) {
    // Idempotent per row (upsert on repositoryId+capturedAt), so a crash mid-loop just
    // means a retry re-diffs against the DB and picks up wherever it left off.
    for (const row of rowsToWrite) {
      await upsertStarSnapshot(qx, repo.repositoryId, row.count, `${row.date}T00:00:00.000Z`)
    }
  }

  return { status: reconciled ? 'reconciled' : 'anchored', daysWritten: rowsToWrite.length }
}

// `concurrency` workers pull the next index as they free up, instead of lockstep batches.
// A worker never abandons a grabbed item, so returned count N means indices 0..N-1 are done.
export async function runWithConcurrency<T>(
  items: T[],
  concurrency: number,
  isShuttingDown: () => boolean,
  handler: (item: T) => Promise<void>,
): Promise<number> {
  let nextIndex = 0

  async function worker(): Promise<void> {
    while (nextIndex < items.length) {
      if (isShuttingDown()) {
        return
      }
      const index = nextIndex++
      if (index >= items.length) {
        return
      }
      await handler(items[index])
    }
  }

  const workerCount = Math.min(concurrency, items.length)
  await Promise.all(Array.from({ length: workerCount }, () => worker()))

  return nextIndex
}

function recordOutcome(totals: StarSnapshotBackfillTotals, result: RepoBackfillResult): void {
  totals.daysWritten += result.daysWritten
  if (result.status === 'skipped-no-history') {
    totals.reposSkippedNoHistory++
  } else if (result.status === 'skipped-negative-count') {
    totals.reposSkippedNegativeCount++
  } else if (result.status === 'reconciled') {
    totals.reposReconciled++
  } else {
    totals.reposAnchoredBackward++
  }
}

export async function runStarSnapshotBackfill(
  qx: QueryExecutor,
  log: Logger,
  options: StarSnapshotBackfillOptions,
): Promise<StarSnapshotBackfillTotals> {
  const totals: StarSnapshotBackfillTotals = {
    reposProcessed: 0,
    reposSkippedAlreadyBackfilled: 0,
    reposSkippedNoHistory: 0,
    reposSkippedNegativeCount: 0,
    reposReconciled: 0,
    reposAnchoredBackward: 0,
    reposFailed: 0,
    reposRecoveredOnRetry: 0,
    daysWritten: 0,
    completed: false,
  }

  const rateLimiter = createCoreRateLimiter(options.reservedCoreRateLimit, log)
  const failedRepos: IRepoForStarSnapshot[] = []
  let afterUrl = options.afterUrl
  // The persisted checkpoint, kept separate from `afterUrl` (the live scan cursor) so a
  // failure doesn't rewind live pagination - only frozen until the retry sweep resolves it.
  let checkpointUrl = options.afterUrl
  let checkpointFrozen = false

  while (!options.isShuttingDown()) {
    const repos = await findReposForStarSnapshot(qx, REPO_PAGE_SIZE, afterUrl)
    if (repos.length === 0) {
      totals.completed = true
      break
    }

    const failedBeforePage = failedRepos.length
    const processedCount = await runWithConcurrency(
      repos,
      options.concurrency,
      options.isShuttingDown,
      async (repo) => {
        if (options.completedRepoIds?.has(repo.repositoryId)) {
          totals.reposSkippedAlreadyBackfilled++
          return
        }

        totals.reposProcessed++
        try {
          const result = await backfillRepo(qx, repo, rateLimiter, log, {
            dryRun: options.dryRun,
            failFast: false,
          })
          // A negative-count skip is a reconstruction anomaly, not a terminal success -
          // leave it off completedRepoIds so a future run retries it instead of skipping forever.
          if (result.status !== 'skipped-negative-count') {
            options.completedRepoIds?.add(repo.repositoryId)
          }
          recordOutcome(totals, result)
        } catch (err) {
          totals.reposFailed++
          failedRepos.push(repo)
          log.warn(
            { repoUrl: repo.repoUrl, error: (err as Error)?.message ?? err },
            'star snapshot backfill failed for repo',
          )
        }

        if (totals.reposProcessed % options.concurrency === 0) {
          log.info({ ...totals, afterUrl: repo.repoUrl }, 'star snapshot backfill batch done')
        }
      },
    )

    if (processedCount === 0) {
      // Shutdown hit before any repo in this page ran - leave the checkpoint pointing
      // at the previous page so none of these repos are skipped on resume.
      break
    }

    // The live cursor always advances a full page so the scan keeps making progress and
    // reaches the retry sweep - only the persisted checkpoint freezes at a failure.
    afterUrl = repos[processedCount - 1].repoUrl

    const newFailures = failedRepos.slice(failedBeforePage)
    if (!checkpointFrozen && newFailures.length > 0) {
      const earliestFailedIndex = Math.min(...newFailures.map((repo) => repos.indexOf(repo)))
      if (earliestFailedIndex > 0) {
        checkpointUrl = repos[earliestFailedIndex - 1].repoUrl
      }
      checkpointFrozen = true
    }
    if (!checkpointFrozen) {
      checkpointUrl = afterUrl
    }

    log.info({ ...totals, afterUrl }, 'star snapshot backfill progress')
    await options.onProgress?.(checkpointUrl, totals)

    if (processedCount < repos.length) {
      break
    }

    if (repos.length < REPO_PAGE_SIZE) {
      totals.completed = true
      break
    }
  }

  // One retry pass over this run's failures, after any rate-limit cooldown - only on a
  // full pass, since a resumed checkpoint never revisits repos behind its cursor.
  if (totals.completed && !options.isShuttingDown() && failedRepos.length > 0) {
    const toRetry = failedRepos.splice(0, failedRepos.length)
    log.info(
      { retryCount: toRetry.length },
      'star snapshot backfill retrying repos that failed earlier this run',
    )

    await runWithConcurrency(toRetry, options.concurrency, options.isShuttingDown, async (repo) => {
      try {
        const result = await backfillRepo(qx, repo, rateLimiter, log, {
          dryRun: options.dryRun,
          failFast: false,
        })
        totals.reposFailed--
        if (result.status !== 'skipped-negative-count') {
          options.completedRepoIds?.add(repo.repositoryId)
          totals.reposRecoveredOnRetry++
        }
        recordOutcome(totals, result)
      } catch (err) {
        log.warn(
          { repoUrl: repo.repoUrl, error: (err as Error)?.message ?? err },
          'star snapshot backfill retry failed for repo',
        )
      }
    })

    log.info({ ...totals }, 'star snapshot backfill retry sweep done')
    // Checkpoint only advances once every failure is resolved, so a still-failing repo
    // stays retryable; onProgress still flushes completedRepoIds for recovered repos either way.
    if (afterUrl && totals.reposFailed === 0) {
      checkpointUrl = afterUrl
    }
    await options.onProgress?.(checkpointUrl, totals)
  }

  return totals
}
