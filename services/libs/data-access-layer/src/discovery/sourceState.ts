import { QueryExecutor } from '../queryExecutor'

export async function findDiscoverySourceWatermark(
  qx: QueryExecutor,
  source: string,
): Promise<string | null> {
  const row: { watermark: string | null } | null = await qx.selectOneOrNone(
    `
    SELECT "watermark"
    FROM public."discoverySourceState"
    WHERE "source" = $(source)
    `,
    { source },
  )

  return row?.watermark ?? null
}

// GREATEST enforces forward-only in SQL, not in the caller. `force` bypasses it —
// used only by mode: 'full' to recover from a bad (e.g. future-dated) watermark.
export async function upsertDiscoverySourceWatermark(
  qx: QueryExecutor,
  source: string,
  watermark: string,
  options: { force?: boolean } = {},
): Promise<void> {
  await qx.result(
    `
    INSERT INTO public."discoverySourceState" ("source", "watermark", "lastRunAt")
    VALUES ($(source), $(watermark), NOW())
    ON CONFLICT ("source") DO UPDATE
    SET
      "watermark" = CASE
        WHEN $(force) THEN EXCLUDED."watermark"
        ELSE GREATEST(public."discoverySourceState"."watermark", EXCLUDED."watermark")
      END,
      "lastRunAt" = NOW(),
      "updatedAt" = NOW()
    `,
    { source, watermark, force: options.force ?? false },
  )
}
