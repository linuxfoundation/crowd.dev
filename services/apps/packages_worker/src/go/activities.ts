import { Context } from '@temporalio/activity'

import { logAuditFieldChanges } from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getGoConfig } from '../config'
import { getPackagesDb } from '../db'

import { fetchStatus } from './pkgGoDevClient'
import { fetchLatest } from './proxyClient'
import { isFetchError } from './types'

const log = getServiceChildLogger('go')

const PROXY_SOURCE = 'go-proxy'
const PKGGODEV_SOURCE = 'pkg-go-dev'

// No purl cursor: is_critical DESC + last_synced_at ASC means every call surfaces the
// highest-priority not-yet-synced-this-run rows first, and rows we just synced sink to the
// back (last_synced_at = NOW()) so the next call naturally picks up where this one left off.
// runStartedAt bounds the run — once every row has last_synced_at >= runStartedAt, this
// returns empty and the workflow ends for the day.
async function getGoBatch(
  qx: QueryExecutor,
  runStartedAt: string,
  batchSize: number,
): Promise<Array<{ purl: string; name: string }>> {
  return qx.select(
    `SELECT purl, name FROM packages
     WHERE ecosystem = 'go' AND (last_synced_at IS NULL OR last_synced_at < $(runStartedAt))
     ORDER BY is_critical DESC, last_synced_at ASC NULLS FIRST, purl ASC
     LIMIT $(limit)`,
    { runStartedAt, limit: batchSize },
  )
}

export async function getGoRunStartedAt(): Promise<string> {
  return new Date().toISOString()
}

export async function enrichGoVersionsBatch(
  runStartedAt: string,
  batchSize: number,
): Promise<number> {
  const qx = await getPackagesDb()
  const rows = await getGoBatch(qx, runStartedAt, batchSize)
  if (rows.length === 0) return 0

  const { fetchTimeoutMs, proxyConcurrency } = getGoConfig()

  const enrichOne = async (row: { purl: string; name: string }): Promise<void> => {
    Context.current().heartbeat(row.purl)
    const result = await fetchLatest(row.name, fetchTimeoutMs)
    if (isFetchError(result)) {
      log.warn(
        { purl: row.purl, name: row.name, kind: result.kind, statusCode: result.statusCode },
        'go proxy fetch failed — skipping package',
      )
      // Touch last_synced_at even on failure so this row sinks behind unprocessed ones and
      // isn't re-selected into every subsequent batch for the rest of this run. It'll be
      // retried on tomorrow's run.
      await qx.result(`UPDATE packages SET last_synced_at = NOW() WHERE purl = $(purl)`, {
        purl: row.purl,
      })
      return
    }
    const changed = await qx.selectOne(
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
        repoUrl: result.repoUrl,
        purl: row.purl,
      },
    )
    const changedFields = [
      changed?.v_changed ? 'packages.latest_version' : null,
      changed?.t_changed ? 'packages.latest_release_at' : null,
      changed?.r_changed ? 'packages.repository_url' : null,
    ].filter(Boolean) as string[]
    await logAuditFieldChanges(qx, PROXY_SOURCE, row.purl, changedFields)
  }

  for (let i = 0; i < rows.length; i += proxyConcurrency) {
    await Promise.all(rows.slice(i, i + proxyConcurrency).map(enrichOne))
  }

  log.info({ count: rows.length, concurrency: proxyConcurrency }, 'Enriched go versions batch')
  return rows.length
}

export async function enrichGoStatusBatch(
  runStartedAt: string,
  batchSize: number,
): Promise<number> {
  const qx = await getPackagesDb()
  const rows = await getGoBatch(qx, runStartedAt, batchSize)
  if (rows.length === 0) return 0

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
      // Touch last_synced_at even on failure so this row sinks behind unprocessed ones and
      // isn't re-selected into every subsequent batch for the rest of this run. It'll be
      // retried on tomorrow's run.
      await qx.result(`UPDATE packages SET last_synced_at = NOW() WHERE purl = $(purl)`, {
        purl: row.purl,
      })
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
  return rows.length
}
