import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { NormalizedRange, NormalizedRecord } from './types'

// Drop duplicate ranges that would collide on the
// (advisory_package_id, COALESCE(introduced_version,'')) unique index.
// We keep the first occurrence; OSV occasionally emits redundant events for
// the same introduced=X line.
function dedupeRanges(ranges: NormalizedRange[]): NormalizedRange[] {
  const seen = new Set<string>()
  const out: NormalizedRange[] = []
  for (const r of ranges) {
    const key = r.introducedVersion ?? ''
    if (seen.has(key)) continue
    seen.add(key)
    out.push(r)
  }
  return out
}

async function upsertOne(qx: QueryExecutor, record: NormalizedRecord): Promise<void> {
  const { advisory, packages } = record
  if (packages.length === 0) return

  const advisoryRow = await qx.selectOne(
    `
    INSERT INTO advisories
      (osv_id, aliases, severity, cvss, cvss_source, summary, details, published_at, modified_at)
    VALUES
      ($(osvId), $(aliases)::text[], $(severity), $(cvss), $(cvssSource),
       $(summary), $(details), $(publishedAt)::timestamptz, $(modifiedAt)::timestamptz)
    ON CONFLICT (osv_id) DO UPDATE SET
      aliases      = EXCLUDED.aliases,
      severity     = EXCLUDED.severity,
      cvss         = EXCLUDED.cvss,
      cvss_source  = EXCLUDED.cvss_source,
      summary      = EXCLUDED.summary,
      details      = EXCLUDED.details,
      published_at = EXCLUDED.published_at,
      modified_at  = EXCLUDED.modified_at
    RETURNING id
    `,
    advisory,
  )
  const advisoryId = advisoryRow.id as number

  for (const entry of packages) {
    const resolved = await qx.selectOneOrNone(
      `
      SELECT id
      FROM packages
      WHERE ecosystem = $(ecosystem)
        AND COALESCE(namespace, '') = COALESCE($(namespace), '')
        AND name = $(name)
      `,
      entry.pkg,
    )
    const packageId: number | null = resolved?.id ?? null

    const advisoryPackageRow = await qx.selectOne(
      `
      INSERT INTO advisory_packages
        (advisory_id, package_id, ecosystem, package_name)
      VALUES
        ($(advisoryId), $(packageId), $(ecosystem), $(packageName))
      ON CONFLICT (advisory_id, ecosystem, package_name) DO UPDATE SET
        package_id = EXCLUDED.package_id
      RETURNING id
      `,
      {
        advisoryId,
        packageId,
        ecosystem: entry.pkg.ecosystem,
        packageName: entry.pkg.packageName,
      },
    )
    const advisoryPackageId = advisoryPackageRow.id as number

    await qx.result(
      `DELETE FROM advisory_affected_ranges WHERE advisory_package_id = $(advisoryPackageId)`,
      { advisoryPackageId },
    )

    const ranges = dedupeRanges(entry.ranges)
    for (const range of ranges) {
      await qx.result(
        `
        INSERT INTO advisory_affected_ranges
          (advisory_package_id, introduced_version, fixed_version, last_affected)
        VALUES
          ($(advisoryPackageId), $(introducedVersion), $(fixedVersion), $(lastAffected))
        `,
        {
          advisoryPackageId,
          introducedVersion: range.introducedVersion,
          fixedVersion: range.fixedVersion,
          lastAffected: range.lastAffected,
        },
      )
    }
  }
}

// upsertAdvisoryBatch writes a batch of normalized OSV records in a single
// transaction. Caller groups into batches of ~OSV_BATCH_SIZE to keep transaction
// overhead amortized without holding too many row locks at once.
export async function upsertAdvisoryBatch(
  qx: QueryExecutor,
  batch: NormalizedRecord[],
): Promise<void> {
  if (batch.length === 0) return
  await qx.tx(async (tx) => {
    for (const record of batch) {
      await upsertOne(tx, record)
    }
  })
}
