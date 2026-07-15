import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'

import { buildBaseDeps, processRepo } from './processBatch'
import { RepoPackage, RepoTarget } from './types'
import { markRepoAttempted } from './writeContacts'

const log = getServiceChildLogger('security-contacts-ondemand')

type Config = ReturnType<typeof getSecurityContactsConfig>

export interface IngestSingleResult {
  found: boolean
  repoId?: string
}

interface SingleRepoRow {
  repoId: string
  url: string
  homepage: string | null
  archived: boolean | null
  packages: RepoPackage[] | null
}

// Mirrors the best-repo LATERAL selection in getPackageDetailByPurl
// (services/libs/data-access-layer/src/osspckgs/api.ts) so the repo we ingest is the
// one the read side surfaces. No `is_critical` filter — non-critical purls are exactly
// what this on-demand path exists for. `packages` aggregates every package linked to
// the repo (not just the requested purl) so extractors see every ecosystem, matching
// the shape the batch sweep builds.
//
// No host filter: processRepo already degrades gracefully for non-github repos (the
// github-specific extractors no-op, security.txt/registry-manifest extractors still run) —
// filtering here would leave non-github repos permanently NULL, re-triggering this
// on-demand path on every single request.
async function findBestRepoForPurl(qx: QueryExecutor, purl: string): Promise<SingleRepoRow | null> {
  return qx.selectOneOrNone(
    `
    SELECT r.id::text AS "repoId",
           r.url,
           r.homepage,
           r.archived,
           (
             SELECT json_agg(json_build_object('purl', p2.purl, 'ecosystem', p2.ecosystem))
             FROM package_repos pr2
             JOIN packages p2 ON p2.id = pr2.package_id
             WHERE pr2.repo_id = r.id
           ) AS packages
    FROM packages p
    JOIN LATERAL (
      SELECT pr.repo_id
      FROM package_repos pr
      WHERE pr.package_id = p.id
      ORDER BY pr.confidence DESC, (pr.source = 'declared') DESC
      LIMIT 1
    ) best ON true
    JOIN repos r ON r.id = best.repo_id
    WHERE p.purl = $(purl)
    `,
    { purl },
  )
}

function toTarget(row: SingleRepoRow): RepoTarget {
  return {
    repoId: row.repoId,
    url: row.url,
    homepage: row.homepage,
    archived: row.archived,
    packages: row.packages ?? [],
  }
}

/**
 * On-demand ingest for a single purl, bypassing the daily critical-only sweep.
 * Used when the akrites API hits a repo that has never been evaluated
 * (repos.contacts_last_refreshed IS NULL).
 */
export async function ingestSecurityContactsForPurl(
  qx: QueryExecutor,
  cdpQx: QueryExecutor,
  config: Config,
  purl: string,
): Promise<IngestSingleResult> {
  const row = await findBestRepoForPurl(qx, purl)
  if (!row) {
    log.info({ purl }, 'On-demand security contacts: no linked repo found — skipping')
    return { found: false }
  }

  const target = toTarget(row)
  const baseDeps = buildBaseDeps(config)
  try {
    await processRepo(target, baseDeps, qx, cdpQx)
  } catch (err) {
    log.error({ repoId: target.repoId, errMsg: (err as Error).message }, 'Repo processing failed')
    await markRepoAttempted(qx, target.repoId).catch(() => undefined)
  }

  return { found: true, repoId: target.repoId }
}
