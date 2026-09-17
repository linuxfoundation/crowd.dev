import { generateUUIDv4 } from '@crowd/common'
import { IRepoForStarSnapshot, IRepositoryStarSnapshot } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

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

// Repos whose captured-day count falls short of their first-to-last day span - a real
// gap, not just "not backfilled yet" (that's findReposNeedingStarBackfill's job).
// Scoped to repositoryIds so this stays an index seek per repo, not a full-table scan.
export async function findRepoIdsWithStarSnapshotGaps(
  qx: QueryExecutor,
  repositoryIds: string[],
): Promise<string[]> {
  if (repositoryIds.length === 0) {
    return []
  }

  const rows: { repositoryId: string }[] = await qx.select(
    `
      with per_repo as (
        select
            "repositoryId",
            min("capturedAt"::date) as first_date,
            max("capturedAt"::date) as last_date,
            count(distinct "capturedAt"::date) as distinct_days
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
        and (p.last_date - p.first_date + 1) - p.distinct_days > 0
    `,
    { repositoryIds },
  )

  return (rows || []).map((row) => row.repositoryId)
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
