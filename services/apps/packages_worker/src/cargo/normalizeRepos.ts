import { QueryExecutor } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { CanonicalRepo, canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'

import { STAGING_SCHEMA } from './loadDump'
import { NormalizeReposResult } from './types'

const log = getServiceChildLogger('cargo-normalize')

// Batch size for the mapping upsert — keeps parameter arrays well under Postgres limits.
const INSERT_BATCH = 5000

/**
 * Builds `cargo_sync.repo_norm(declared, repository_url, host)`, mapping every
 * distinct `declared_repository_url` staged in `enrich_packages` to its canonical
 * `https://<host>/<owner>/<name>` form via the shared `canonicalizeRepoUrl`
 * (same normalizer npm/pypi use — no per-ecosystem fork).
 *
 * Only successfully-canonicalized inputs are stored; non-repository values
 * (placeholders, homepages, free-form text) are omitted, so a LEFT JOIN from
 * `enrich_packages` yields NULL — that both recovers derivable URLs (Gap A) and
 * clears junk that could otherwise land in `packages.repository_url` (Gap C).
 *
 * Normalization runs in TypeScript because the bulk set-based cargo pipeline
 * cannot call the parser per row; the resulting mapping table lets the SQL
 * enrich phases join back to it. Idempotent: the table is rebuilt each run.
 */
export async function normalizeRepos(qx: QueryExecutor): Promise<NormalizeReposResult> {
  const rows: Array<{ declared_repository_url: string }> = await qx.select(
    `SELECT DISTINCT declared_repository_url
       FROM ${STAGING_SCHEMA}.enrich_packages
      WHERE declared_repository_url IS NOT NULL`,
  )

  await qx.result(
    `DROP TABLE IF EXISTS ${STAGING_SCHEMA}.repo_norm CASCADE;
     CREATE TABLE ${STAGING_SCHEMA}.repo_norm (
       declared       text PRIMARY KEY,
       repository_url text NOT NULL,
       host           text NOT NULL
     )`,
  )

  const mapped = rows
    .map(({ declared_repository_url }) => ({
      declared: declared_repository_url,
      canonical: canonicalizeRepoUrl(declared_repository_url),
    }))
    .filter((r): r is { declared: string; canonical: CanonicalRepo } => r.canonical !== null)

  for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
    const batch = mapped.slice(i, i + INSERT_BATCH)
    await qx.result(
      `INSERT INTO ${STAGING_SCHEMA}.repo_norm (declared, repository_url, host)
       SELECT * FROM unnest($(declared)::text[], $(urls)::text[], $(hosts)::text[])
       ON CONFLICT (declared) DO NOTHING`,
      {
        declared: batch.map((r) => r.declared),
        urls: batch.map((r) => r.canonical.url),
        hosts: batch.map((r) => r.canonical.host),
      },
    )
  }

  const result: NormalizeReposResult = { scanned: rows.length, normalized: mapped.length }
  log.info(result, 'cargo repo normalization complete')
  return result
}
