import { MergeActionType, MergeSuggestionSource } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export async function insertMergeSuggestionSources(
  qx: QueryExecutor,
  type: MergeActionType,
  source: MergeSuggestionSource,
  pairs: [string, string][],
): Promise<void> {
  if (pairs.length === 0) {
    return
  }

  await qx.result(
    `
      insert into "mergeSuggestionSources" ("type", "primaryId", "secondaryId", "source")
      select $(type), least(p.a, p.b), greatest(p.a, p.b), $(source)
      from unnest($(firstIds)::uuid[], $(secondIds)::uuid[]) as p(a, b)
      on conflict do nothing
    `,
    {
      type,
      source,
      firstIds: pairs.map(([first]) => first),
      secondIds: pairs.map(([, second]) => second),
    },
  )
}
