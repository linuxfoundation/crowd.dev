import { IRepositoryStarBackfillStatus } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { truncateErrorMessage } from '../utils'

export async function recordStarBackfillFailure(
  qx: QueryExecutor,
  repositoryId: string,
  errorClass: string,
  errorMessage: string | null,
  deadLetterAfter: number | null,
): Promise<void> {
  await qx.result(
    `
      insert into public."repositoryStarBackfillStatus"
          ("repositoryId", "consecutiveFailures", "lastErrorClass", "lastErrorMessage", "deadLetteredAt", "createdAt", "updatedAt")
      values
          ($(repositoryId), 1, $(errorClass), $(errorMessage),
           case when $(deadLetterAfter)::int is not null and 1 >= $(deadLetterAfter) then now() else null end,
           now(), now())
      on conflict ("repositoryId")
          do update
          set "consecutiveFailures" = "repositoryStarBackfillStatus"."consecutiveFailures" + 1,
              "lastErrorClass" = excluded."lastErrorClass",
              "lastErrorMessage" = excluded."lastErrorMessage",
              "deadLetteredAt" = case
                  when "repositoryStarBackfillStatus"."deadLetteredAt" is not null
                      then "repositoryStarBackfillStatus"."deadLetteredAt"
                  when $(deadLetterAfter)::int is not null
                      and "repositoryStarBackfillStatus"."consecutiveFailures" + 1 >= $(deadLetterAfter)
                      then now()
                  else null
              end,
              "completedAt" = null,
              "updatedAt" = now()
    `,
    {
      repositoryId,
      errorClass,
      errorMessage: truncateErrorMessage(errorMessage),
      deadLetterAfter,
    },
  )
}

export async function recordStarBackfillSuccess(
  qx: QueryExecutor,
  repositoryId: string,
): Promise<void> {
  await qx.result(
    `
      insert into public."repositoryStarBackfillStatus"
          ("repositoryId", "consecutiveFailures", "completedAt", "createdAt", "updatedAt")
      values
          ($(repositoryId), 0, now(), now(), now())
      on conflict ("repositoryId")
          do update
          set "consecutiveFailures" = 0,
              "lastErrorClass" = null,
              "lastErrorMessage" = null,
              "deadLetteredAt" = null,
              "completedAt" = now(),
              "updatedAt" = now()
    `,
    { repositoryId },
  )
}

export async function findDeadLetteredStarBackfillFailures(
  qx: QueryExecutor,
  // string accepted so a caller can pass back a previously-read `deadLetteredAt` (pg's raw
  // text output) unparsed - reparsing it via `new Date()` is session-timezone dependent.
  since: Date | string | null = null,
): Promise<IRepositoryStarBackfillStatus[]> {
  const failures: IRepositoryStarBackfillStatus[] = await qx.select(
    `
      select f.*
      from public."repositoryStarBackfillStatus" f
      join public.repositories r on r.id = f."repositoryId"
      where f."deadLetteredAt" is not null
        and ($(since)::timestamptz is null or f."deadLetteredAt" > $(since))
        and r."deletedAt" is null
        and r."excluded" = false
      order by f."deadLetteredAt" desc
    `,
    { since },
  )

  return failures || []
}

// DB-time watermark, not a max-observed-row value - a transaction's `now()` is its start time,
// so a slow commit can land with an older timestamp than a cursor set from an already-read row.
export async function getDeadLetterReportCursor(qx: QueryExecutor): Promise<string> {
  const { cursor } = await qx.selectOne(
    `select (now() - interval '5 minutes')::timestamptz as cursor`,
  )

  return cursor
}

export async function countDeadLetteredStarBackfillFailures(qx: QueryExecutor): Promise<number> {
  const { count } = await qx.selectOne(
    `
      select count(*)::int as count
      from public."repositoryStarBackfillStatus" f
      join public.repositories r on r.id = f."repositoryId"
      where f."deadLetteredAt" is not null
        and r."deletedAt" is null
        and r."excluded" = false
    `,
  )

  return count
}
