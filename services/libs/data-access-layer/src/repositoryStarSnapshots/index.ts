import { generateUUIDv4 } from '@crowd/common'
import { IRepoForStarSnapshot, IRepositoryStarSnapshot } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export async function findReposForStarSnapshot(
  qx: QueryExecutor,
  limit = 1000,
): Promise<IRepoForStarSnapshot[]> {
  const repos: IRepoForStarSnapshot[] = await qx.select(
    `
      select
          r.id as "repositoryId",
          r.url as "repoUrl",
          nm."connectionId" as "connectionId"
      from public.repositories r
      join integration.nango_mapping nm on nm."repositoryId" = r.id
      where r."deletedAt" is null
        and r."excluded" = false
        and r.url like 'https://github.com%'
      order by r.url asc
      limit $(limit)
    `,
    { limit },
  )

  return repos || []
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
