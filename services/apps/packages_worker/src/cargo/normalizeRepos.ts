import { QueryExecutor } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { CanonicalRepo, canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'
import { repoOwnerFromCanonical } from '../utils/ownershipMatch'

import { STAGING_SCHEMA } from './loadDump'
import { NormalizeReposResult } from './types'

const log = getServiceChildLogger('cargo-normalize')

// Batch size for the mapping upsert — keeps parameter arrays well under Postgres limits.
const INSERT_BATCH = 5000

export async function normalizeRepos(qx: QueryExecutor): Promise<NormalizeReposResult> {
  const rows: Array<{ url: string }> = await qx.select(
    `SELECT DISTINCT declared_repository_url AS url
       FROM ${STAGING_SCHEMA}.enrich_packages
      WHERE declared_repository_url IS NOT NULL
     UNION
     SELECT DISTINCT homepage AS url
       FROM ${STAGING_SCHEMA}.enrich_packages
      WHERE homepage IS NOT NULL`,
  )

  await qx.result(
    `DROP TABLE IF EXISTS ${STAGING_SCHEMA}.repo_norm CASCADE;
     CREATE TABLE ${STAGING_SCHEMA}.repo_norm (
       declared       text PRIMARY KEY,
       repository_url text NOT NULL,
       host           text NOT NULL,
       owner          text
     )`,
  )

  const mapped = rows
    .map(({ url }) => ({
      declared: url,
      canonical: canonicalizeRepoUrl(url),
    }))
    .filter((r): r is { declared: string; canonical: CanonicalRepo } => r.canonical !== null)

  for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
    const batch = mapped.slice(i, i + INSERT_BATCH)
    await qx.result(
      `INSERT INTO ${STAGING_SCHEMA}.repo_norm (declared, repository_url, host, owner)
       SELECT * FROM unnest($(declared)::text[], $(urls)::text[], $(hosts)::text[], $(owners)::text[])
       ON CONFLICT (declared) DO NOTHING`,
      {
        declared: batch.map((r) => r.declared),
        urls: batch.map((r) => r.canonical.url),
        hosts: batch.map((r) => r.canonical.host),
        owners: batch.map((r) => repoOwnerFromCanonical(r.canonical)),
      },
    )
  }

  await qx.result(
    `DROP TABLE IF EXISTS ${STAGING_SCHEMA}.repo_choice CASCADE;
     CREATE TABLE ${STAGING_SCHEMA}.repo_choice AS
     SELECT e.package_id,
            COALESCE(rd.repository_url, rh.repository_url) AS repository_url,
            COALESCE(rd.host, rh.host)                     AS host,
            COALESCE(rd.owner, rh.owner)                   AS owner,
            CASE WHEN rd.repository_url IS NOT NULL THEN 'primary' ELSE 'secondary' END AS signal
     FROM ${STAGING_SCHEMA}.enrich_packages e
     LEFT JOIN ${STAGING_SCHEMA}.repo_norm rd ON rd.declared = e.declared_repository_url
     LEFT JOIN ${STAGING_SCHEMA}.repo_norm rh ON rh.declared = e.homepage AND rh.host <> 'other';
     CREATE INDEX ON ${STAGING_SCHEMA}.repo_choice (package_id)`,
  )

  const choiceRow = await qx.selectOne(
    `SELECT COUNT(*) FILTER (WHERE repository_url IS NOT NULL AND signal = 'secondary')::int AS fallbacks
       FROM ${STAGING_SCHEMA}.repo_choice`,
  )

  const result: NormalizeReposResult = {
    scanned: rows.length,
    normalized: mapped.length,
    homepageFallbacks: choiceRow.fallbacks,
  }
  log.info(result, 'cargo repo normalization complete')
  return result
}
