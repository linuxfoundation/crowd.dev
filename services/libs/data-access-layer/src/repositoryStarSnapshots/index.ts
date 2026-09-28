import { generateUUIDv4 } from '@crowd/common'
import { IRepoForStarSnapshot, IRepositoryStarSnapshot } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { batchAll } from '../utils'

export * from './backfillStatus'

export async function findReposForStarSnapshot(
  qx: QueryExecutor,
  limit: number | null = null,
  afterUrl: string | null = null,
): Promise<IRepoForStarSnapshot[]> {
  const repos: IRepoForStarSnapshot[] = await qx.select(
    `
      select
          r.id as "repositoryId",
          r.url as "repoUrl"
      from public.repositories r
      where r."deletedAt" is null
        and r."excluded" = false
        and r.url like 'https://github.com%'
        and ($(afterUrl)::text is null or r.url > $(afterUrl))
      order by r.url asc
      limit $(limit)
    `,
    { limit, afterUrl },
  )

  return repos || []
}

export async function findReposNeedingStarBackfill(
  qx: QueryExecutor,
  limit: number | null = null,
  afterUrl: string | null = null,
): Promise<IRepoForStarSnapshot[]> {
  const repos: IRepoForStarSnapshot[] = await qx.select(
    `
      select
          r.id as "repositoryId",
          r.url as "repoUrl"
      from public.repositories r
      left join public."repositoryStarBackfillStatus" f on f."repositoryId" = r.id
      where r."deletedAt" is null
        and r."excluded" = false
        and r.url like 'https://github.com%'
        and ($(afterUrl)::text is null or r.url > $(afterUrl))
        and f."deadLetteredAt" is null
        and f."completedAt" is null
      order by r.url asc
      limit $(limit)
    `,
    { limit, afterUrl },
  )

  return repos || []
}

// Repos whose captured-day count falls short of their day span (real gap, not just
// "not backfilled yet" - findReposNeedingStarBackfill's job). Scoped for an index seek.
export async function findRepoIdsWithStarSnapshotGaps(
  qx: QueryExecutor,
  repositoryIds: string[],
): Promise<string[]> {
  if (repositoryIds.length === 0) {
    return []
  }

  // AT TIME ZONE 'UTC' before the date cast - capturedAt's raw string form renders in the
  // session's timezone, and GitHub's history/backfill writes are UTC-day based.
  const rows: { repositoryId: string }[] = await qx.select(
    `
      with per_repo as (
        select
            "repositoryId",
            min(("capturedAt" at time zone 'UTC')::date) as first_date,
            max(("capturedAt" at time zone 'UTC')::date) as last_date,
            count(distinct ("capturedAt" at time zone 'UTC')::date) as distinct_days
        from "repositoryStarSnapshots"
        where "repositoryId" in ($(repositoryIds:csv))
        group by "repositoryId"
      )
      select p."repositoryId"
      from per_repo p
      join public.repositories r on r.id = p."repositoryId"
      where r."deletedAt" is null
        and r."excluded" = false
        and r.url like 'https://github.com%'
        and (
          (p.last_date - p.first_date + 1) - p.distinct_days > 0
          or p.last_date < (now() at time zone 'UTC')::date - 1
        )
    `,
    { repositoryIds },
  )

  return (rows || []).map((row) => row.repositoryId)
}

const DEFAULT_GAP_CHECK_BATCH_SIZE = 5_000

// Chunks the IN-list so it stays bounded as the eligible repo count grows.
export async function findAllRepoIdsWithStarSnapshotGaps(
  qx: QueryExecutor,
  repositoryIds: string[],
  batchSize: number = DEFAULT_GAP_CHECK_BATCH_SIZE,
): Promise<string[]> {
  return batchAll(repositoryIds, batchSize, (batch) => findRepoIdsWithStarSnapshotGaps(qx, batch))
}

// Completed, still-retryable repos - the population findReposNeedingStarBackfill skips.
// Paginated the same way, so a caller can bound each page's gap check (findAllRepoIdsWithStarSnapshotGaps).
export async function findCompletedReposEligibleForGapHeal(
  qx: QueryExecutor,
  limit: number | null = null,
  afterUrl: string | null = null,
): Promise<IRepoForStarSnapshot[]> {
  const repos: IRepoForStarSnapshot[] = await qx.select(
    `
      select
          r.id as "repositoryId",
          r.url as "repoUrl"
      from public.repositories r
      join public."repositoryStarBackfillStatus" f on f."repositoryId" = r.id
      where r."deletedAt" is null
        and r."excluded" = false
        and r.url like 'https://github.com%'
        and ($(afterUrl)::text is null or r.url > $(afterUrl))
        and f."completedAt" is not null
        and f."deadLetteredAt" is null
      order by r.url asc
      limit $(limit)
    `,
    { limit, afterUrl },
  )

  return repos || []
}

// Repos with zero rows in repositoryStarSnapshots at all - not "gapped" (that's diffed
// against existing rows), just never captured yet. Scoped for an index seek like the gap check.
export async function findRepoIdsWithoutStarSnapshots(
  qx: QueryExecutor,
  repositoryIds: string[],
): Promise<string[]> {
  if (repositoryIds.length === 0) {
    return []
  }

  const rows: { repositoryId: string }[] = await qx.select(
    `
      select r.id as "repositoryId"
      from public.repositories r
      where r.id in ($(repositoryIds:csv))
        and not exists (
          select 1 from "repositoryStarSnapshots" s where s."repositoryId" = r.id
        )
    `,
    { repositoryIds },
  )

  return (rows || []).map((row) => row.repositoryId)
}

const DEFAULT_NO_SNAPSHOT_CHECK_BATCH_SIZE = 5_000

// Chunks the IN-list so it stays bounded as the eligible repo count grows.
export async function findAllRepoIdsWithoutStarSnapshots(
  qx: QueryExecutor,
  repositoryIds: string[],
  batchSize: number = DEFAULT_NO_SNAPSHOT_CHECK_BATCH_SIZE,
): Promise<string[]> {
  return batchAll(repositoryIds, batchSize, (batch) => findRepoIdsWithoutStarSnapshots(qx, batch))
}

export interface IRepoDaysSinceAdded {
  repositoryId: string
  daysSinceAdded: number
}

// For repos confirmed to have zero snapshot rows - nothing to diff against yet, so this uses
// the repo's own createdAt instead of the gap-days math in findStarSnapshotGapDaysForRepos.
export async function findDaysSinceAddedForRepos(
  qx: QueryExecutor,
  repositoryIds: string[],
): Promise<IRepoDaysSinceAdded[]> {
  if (repositoryIds.length === 0) {
    return []
  }

  const rows: IRepoDaysSinceAdded[] = await qx.select(
    `
      select
          id as "repositoryId",
          ((now() at time zone 'UTC')::date - ("createdAt" at time zone 'UTC')::date)::int as "daysSinceAdded"
      from public.repositories
      where id in ($(repositoryIds:csv))
    `,
    { repositoryIds },
  )

  return rows || []
}

export interface IRepoStarSnapshotGapDays {
  repositoryId: string
  missingDays: number
}

// Caller must pass only repositoryIds already confirmed gapped (e.g. via
// findRepoIdsWithStarSnapshotGaps) - this doesn't re-check gap membership, just sizes it.
export async function findStarSnapshotGapDaysForRepos(
  qx: QueryExecutor,
  repositoryIds: string[],
): Promise<IRepoStarSnapshotGapDays[]> {
  if (repositoryIds.length === 0) {
    return []
  }

  const rows: IRepoStarSnapshotGapDays[] = await qx.select(
    `
      select
          "repositoryId",
          ((greatest(
              max(("capturedAt" at time zone 'UTC')::date),
              (now() at time zone 'UTC')::date - 1
            ) - min(("capturedAt" at time zone 'UTC')::date) + 1)
            - count(distinct ("capturedAt" at time zone 'UTC')::date))::int as "missingDays"
      from "repositoryStarSnapshots"
      where "repositoryId" in ($(repositoryIds:csv))
      group by "repositoryId"
    `,
    { repositoryIds },
  )

  return rows || []
}

export async function upsertStarSnapshot(
  qx: QueryExecutor,
  repositoryId: string,
  starCount: number,
  capturedAt: string,
): Promise<void> {
  await qx.result(
    `
        insert into "repositoryStarSnapshots"
            ("id", "repositoryId", "starCount", "capturedAt", "createdAt", "updatedAt")
        values
            ($(id), $(repositoryId), $(starCount), $(capturedAt), now(), now())
        on conflict ("repositoryId", "capturedAt")
            do update
            set "updatedAt"  = EXCLUDED."updatedAt",
                "starCount"  = EXCLUDED."starCount"
    `,
    {
      id: generateUUIDv4(),
      repositoryId,
      starCount,
      capturedAt,
    },
  )
}

export async function findLatestStarSnapshotForRepo(
  qx: QueryExecutor,
  repositoryId: string,
): Promise<IRepositoryStarSnapshot | null> {
  return qx.selectOneOrNone(
    `
      select *
      from "repositoryStarSnapshots"
      where "repositoryId" = $(repositoryId)
      order by "capturedAt" desc
      limit 1
    `,
    {
      repositoryId,
    },
  )
}

export async function findEarliestStarSnapshotForRepo(
  qx: QueryExecutor,
  repositoryId: string,
): Promise<IRepositoryStarSnapshot | null> {
  return qx.selectOneOrNone(
    `
      select *
      from "repositoryStarSnapshots"
      where "repositoryId" = $(repositoryId)
      order by "capturedAt" asc
      limit 1
    `,
    {
      repositoryId,
    },
  )
}

export async function findStarSnapshotsForRepos(
  qx: QueryExecutor,
  repositoryIds: string[],
  dateRange: { from: string; to: string },
): Promise<IRepositoryStarSnapshot[]> {
  if (repositoryIds.length === 0) {
    return []
  }

  const snapshots: IRepositoryStarSnapshot[] = await qx.select(
    `
      select *
      from "repositoryStarSnapshots"
      where "repositoryId" in ($(repositoryIds:csv))
        and "capturedAt" >= $(from)
        and "capturedAt" <= $(to)
      order by "repositoryId" asc, "capturedAt" asc
    `,
    {
      repositoryIds,
      from: dateRange.from,
      to: dateRange.to,
    },
  )

  return snapshots || []
}
