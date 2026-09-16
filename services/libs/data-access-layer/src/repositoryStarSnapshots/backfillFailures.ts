import { IRepositoryStarBackfillFailure } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

const ERROR_MESSAGE_MAX_LENGTH = 500

function truncateErrorMessage(message: string | null): string | null {
  if (!message) {
    return null
  }
  return message.slice(0, ERROR_MESSAGE_MAX_LENGTH)
}

export async function recordStarBackfillFailure(
  qx: QueryExecutor,
  repositoryId: string,
  errorClass: string,
  errorMessage: string | null,
  deadLetterAfter: number | null,
): Promise<void> {
  await qx.result(
    `
      insert into public."repositoryStarBackfillFailures"
          ("repositoryId", "consecutiveFailures", "lastErrorClass", "lastErrorMessage", "deadLetteredAt", "createdAt", "updatedAt")
      values
          ($(repositoryId), 1, $(errorClass), $(errorMessage),
           case when $(deadLetterAfter)::int is not null and 1 >= $(deadLetterAfter) then now() else null end,
           now(), now())
      on conflict ("repositoryId")
          do update
          set "consecutiveFailures" = "repositoryStarBackfillFailures"."consecutiveFailures" + 1,
              "lastErrorClass" = excluded."lastErrorClass",
              "lastErrorMessage" = excluded."lastErrorMessage",
              "deadLetteredAt" = case
                  when "repositoryStarBackfillFailures"."deadLetteredAt" is not null
                      then "repositoryStarBackfillFailures"."deadLetteredAt"
                  when $(deadLetterAfter)::int is not null
                      and "repositoryStarBackfillFailures"."consecutiveFailures" + 1 >= $(deadLetterAfter)
                      then now()
                  else null
              end,
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
      delete from public."repositoryStarBackfillFailures"
      where "repositoryId" = $(repositoryId)
    `,
    { repositoryId },
  )
}

export async function findDeadLetteredStarBackfillFailures(
  qx: QueryExecutor,
  since: Date | null = null,
): Promise<IRepositoryStarBackfillFailure[]> {
  const failures: IRepositoryStarBackfillFailure[] = await qx.select(
    `
      select *
      from public."repositoryStarBackfillFailures"
      where "deadLetteredAt" is not null
        and ($(since)::timestamptz is null or "deadLetteredAt" >= $(since))
      order by "deadLetteredAt" desc
    `,
    { since },
  )

  return failures || []
}
