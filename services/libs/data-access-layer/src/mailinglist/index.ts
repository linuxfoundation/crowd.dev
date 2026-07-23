import { QueryExecutor } from '../queryExecutor'

export interface IMailingListToOnboard {
  name: string
  sourceUrl: string
}

/**
 * Take a transaction-scoped advisory lock per sourceUrl, so two concurrent
 * connect/update calls touching the same sourceUrl serialize instead of both
 * passing `findMailingListsOwnedByOtherIntegration`'s check and then racing
 * in `upsertMailingLists`'s `ON CONFLICT DO UPDATE` (which silently
 * reassigns ownership to whichever transaction commits last). Locks release
 * automatically at commit/rollback. Call before the ownership check, on a
 * `qx` bound to the same transaction as the rest of the connect flow.
 * @param qx - Query executor (must be transactional)
 * @param sourceUrls - sourceUrls about to be checked/upserted
 */
export async function lockMailingListSourceUrls(
  qx: QueryExecutor,
  sourceUrls: string[],
): Promise<void> {
  // Sorted so overlapping concurrent requests always acquire locks in the
  // same order, avoiding a deadlock between two multi-list connects.
  for (const sourceUrl of [...sourceUrls].sort()) {
    await qx.selectNone(`SELECT pg_advisory_xact_lock(hashtext($(sourceUrl))::bigint)`, {
      sourceUrl,
    })
  }
}

/**
 * Find sourceUrls already onboarded under a different, non-deleted
 * integration. Used to reject a connect/update call before it silently
 * re-points an already-owned list to the calling integration.
 * @param qx - Query executor
 * @param integrationId - Integration attempting to onboard `lists`
 * @param lists - Mailing lists the caller wants to onboard
 */
export async function findMailingListsOwnedByOtherIntegration(
  qx: QueryExecutor,
  integrationId: string,
  lists: IMailingListToOnboard[],
): Promise<string[]> {
  if (lists.length === 0) {
    return []
  }

  const rows = await qx.select(
    `
    SELECT l."sourceUrl"
    FROM mailinglist.lists l
    JOIN public.integrations i ON i.id = l."integrationId"
    WHERE l."integrationId" != $(integrationId)::uuid
      AND l."deletedAt" IS NULL
      AND i."deletedAt" IS NULL
      AND l."sourceUrl" IN (
        SELECT v."sourceUrl" FROM json_to_recordset($(lists)::json) AS v(name text, "sourceUrl" text)
      )
    `,
    {
      integrationId,
      lists: JSON.stringify(lists),
    },
  )

  return rows.map((row: { sourceUrl: string }) => row.sourceUrl)
}

/**
 * Upsert mailing lists (public-inbox/lore) for a segment/integration and
 * seed their processing state so the mailing_list_integration worker picks
 * them up. Re-running with the same sourceUrl re-points the list at the
 * given segment/integration without resetting its processing progress. Any
 * of this integration's lists no longer present in `lists` are soft-deleted,
 * so the worker (which filters on "deletedAt" IS NULL) stops polling them.
 * @param qx - Query executor (should be transactional)
 * @param segmentId - Segment the lists belong to
 * @param integrationId - Integration these lists are onboarded under
 * @param lists - Mailing lists to onboard, keyed by sourceUrl
 */
export async function upsertMailingLists(
  qx: QueryExecutor,
  segmentId: string,
  integrationId: string,
  lists: IMailingListToOnboard[],
): Promise<string[]> {
  if (lists.length === 0) {
    return []
  }

  const rows = await qx.select(
    `
    WITH upserted_list AS (
      INSERT INTO mailinglist.lists (id, name, "sourceUrl", "segmentId", "integrationId", "createdAt", "updatedAt")
      SELECT uuid_generate_v4(), v.name, v."sourceUrl", $(segmentId)::uuid, $(integrationId)::uuid, NOW(), NOW()
      FROM json_to_recordset($(lists)::json) AS v(name text, "sourceUrl" text)
      ON CONFLICT ("sourceUrl") DO UPDATE SET
        name = EXCLUDED.name,
        "segmentId" = EXCLUDED."segmentId",
        "integrationId" = EXCLUDED."integrationId",
        "deletedAt" = NULL,
        "updatedAt" = NOW()
      RETURNING id
    ),
    seed_processing AS (
      INSERT INTO mailinglist."listProcessing" ("listId", state, priority, "lastProcessedHeads", "createdAt", "updatedAt")
      SELECT id, 'pending', 2, '{}'::jsonb, NOW(), NOW()
      FROM upserted_list
      ON CONFLICT ("listId") DO NOTHING
    ),
    removed_lists AS (
      UPDATE mailinglist.lists
      SET "deletedAt" = NOW(), "updatedAt" = NOW()
      WHERE "integrationId" = $(integrationId)::uuid
        AND "deletedAt" IS NULL
        AND "sourceUrl" NOT IN (
          SELECT v."sourceUrl" FROM json_to_recordset($(lists)::json) AS v(name text, "sourceUrl" text)
        )
    )
    SELECT id FROM upserted_list
    `,
    {
      segmentId,
      integrationId,
      lists: JSON.stringify(lists),
    },
  )

  return rows.map((row: { id: string }) => row.id)
}
