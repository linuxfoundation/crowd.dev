import { Context } from '@temporalio/activity'

import {
  getOrCreateRepoByUrl,
  logAuditFieldChanges,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getGoConfig } from '../config'
import { getPackagesDb } from '../db'
import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'

import { fetchStatus } from './pkgGoDevClient'
import { fetchLatest } from './proxyClient'
import { isFetchError } from './types'

const log = getServiceChildLogger('go')

const PROXY_SOURCE = 'go-proxy'
const PKGGODEV_SOURCE = 'pkg-go-dev'

export interface GoScanCursor {
  criticalAfter: string
  after: string
}

type GoRow = { id: string; purl: string; name: string }

// Two independent purl-keyset cursors — one for critical packages, one for everything else —
// each ordered/paginated purely by purl so WHERE and ORDER BY always match (no gaps, no
// duplicates). A single query sorted by is_critical DESC with one shared purl cursor was tried
// and rejected: the cursor advances to the last row's purl, and when a batch is critical-heavy
// that purl can be far ahead of unprocessed non-critical rows, permanently excluding them for
// the rest of the run.
async function getGoBatch(
  qx: QueryExecutor,
  isCritical: boolean,
  afterPurl: string,
  batchSize: number,
): Promise<GoRow[]> {
  if (batchSize <= 0) return []
  return qx.select(
    `SELECT id::text AS id, purl, name FROM packages
     WHERE ecosystem = 'go' AND is_critical = $(isCritical) AND purl > $(after)
     ORDER BY purl ASC
     LIMIT $(limit)`,
    { isCritical, after: afterPurl, limit: batchSize },
  )
}

// Drains not-yet-processed critical packages first, then tops up the rest of the batch with
// non-critical ones — so a rate-limit run only ever starves the non-critical tail.
async function getGoPriorityBatch(
  qx: QueryExecutor,
  cursor: GoScanCursor,
  batchSize: number,
): Promise<{ rows: GoRow[]; nextCursor: GoScanCursor }> {
  const critical = await getGoBatch(qx, true, cursor.criticalAfter, batchSize)
  const nonCritical = await getGoBatch(qx, false, cursor.after, batchSize - critical.length)

  return {
    rows: [...critical, ...nonCritical],
    nextCursor: {
      criticalAfter:
        critical.length > 0 ? critical[critical.length - 1].purl : cursor.criticalAfter,
      after: nonCritical.length > 0 ? nonCritical[nonCritical.length - 1].purl : cursor.after,
    },
  }
}

export async function enrichGoVersionsBatch(
  cursor: GoScanCursor,
  batchSize: number,
): Promise<GoScanCursor | null> {
  const qx = await getPackagesDb()
  const { rows, nextCursor } = await getGoPriorityBatch(qx, cursor, batchSize)
  if (rows.length === 0) return null

  const { fetchTimeoutMs, proxyConcurrency } = getGoConfig()

  const enrichOne = async (row: GoRow): Promise<void> => {
    Context.current().heartbeat(row.purl)
    const result = await fetchLatest(row.name, fetchTimeoutMs)
    if (isFetchError(result)) {
      log.warn(
        { purl: row.purl, name: row.name, kind: result.kind, statusCode: result.statusCode },
        'go proxy fetch failed — skipping package',
      )
      return
    }
    const repo = result.repoUrl ? canonicalizeRepoUrl(result.repoUrl) : null

    await qx.tx(async (t) => {
      const changed = await t.selectOne(
        `WITH old AS (
           SELECT latest_version AS v, latest_release_at AS t, repository_url AS r
           FROM packages WHERE purl = $(purl)
         ),
         upd AS (
           UPDATE packages p SET
             latest_version    = $(version),
             latest_release_at = $(releaseAt),
             repository_url    = COALESCE($(repoUrl), p.repository_url),
             last_synced_at    = NOW()
           WHERE p.purl = $(purl)
           RETURNING latest_version AS v, latest_release_at AS t, repository_url AS r
         )
         SELECT
           (SELECT v FROM old) IS DISTINCT FROM (SELECT v FROM upd) AS v_changed,
           (SELECT t FROM old) IS DISTINCT FROM (SELECT t FROM upd) AS t_changed,
           (SELECT r FROM old) IS DISTINCT FROM (SELECT r FROM upd) AS r_changed`,
        {
          version: result.version,
          releaseAt: result.releaseAt,
          repoUrl: repo?.url ?? null,
          purl: row.purl,
        },
      )
      const changedFields = [
        changed?.v_changed ? 'packages.latest_version' : null,
        changed?.t_changed ? 'packages.latest_release_at' : null,
        changed?.r_changed ? 'packages.repository_url' : null,
      ].filter(Boolean) as string[]

      if (repo) {
        const { id: repoId, changedFields: repoChanged } = await getOrCreateRepoByUrl(
          t,
          repo.url,
          repo.host,
        )
        changedFields.push(...repoChanged)

        const linkChanged = await upsertPackageRepo(t, row.id, repoId, 'declared', 0.8)
        changedFields.push(...linkChanged)
      }

      await logAuditFieldChanges(t, PROXY_SOURCE, row.purl, changedFields)
    })
  }

  for (let i = 0; i < rows.length; i += proxyConcurrency) {
    await Promise.all(rows.slice(i, i + proxyConcurrency).map(enrichOne))
  }

  log.info({ count: rows.length, concurrency: proxyConcurrency }, 'Enriched go versions batch')
  return nextCursor
}

export async function enrichGoStatusBatch(
  cursor: GoScanCursor,
  batchSize: number,
): Promise<GoScanCursor | null> {
  const qx = await getPackagesDb()
  const { rows, nextCursor } = await getGoPriorityBatch(qx, cursor, batchSize)
  if (rows.length === 0) return null

  const { fetchTimeoutMs } = getGoConfig()
  for (const row of rows) {
    const result = await fetchStatus(row.name, fetchTimeoutMs, () =>
      Context.current().heartbeat(row.purl),
    )
    if (isFetchError(result)) {
      log.warn(
        { purl: row.purl, name: row.name, kind: result.kind, statusCode: result.statusCode },
        'pkg.go.dev fetch failed — skipping package',
      )
      continue
    }
    const changed = await qx.selectOne(
      `WITH old AS (
         SELECT status AS s, versions_count AS vc FROM packages WHERE purl = $(purl)
       ),
       upd AS (
         UPDATE packages p SET
           status         = $(status),
           versions_count = COALESCE($(versionsCount), p.versions_count),
           last_synced_at = NOW()
         WHERE p.purl = $(purl)
         RETURNING status AS s, versions_count AS vc
       )
       SELECT
         (SELECT s FROM old)  IS DISTINCT FROM (SELECT s FROM upd)  AS s_changed,
         (SELECT vc FROM old) IS DISTINCT FROM (SELECT vc FROM upd) AS vc_changed`,
      { status: result.status, versionsCount: result.versionsCount, purl: row.purl },
    )
    const changedFields = [
      changed?.s_changed ? 'packages.status' : null,
      changed?.vc_changed ? 'packages.versions_count' : null,
    ].filter(Boolean) as string[]
    await logAuditFieldChanges(qx, PKGGODEV_SOURCE, row.purl, changedFields)
  }

  log.info({ count: rows.length }, 'Enriched go status batch')
  return nextCursor
}
