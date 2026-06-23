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

async function getGoBatch(
  qx: QueryExecutor,
  afterPurl: string,
  batchSize: number,
): Promise<Array<{ purl: string; name: string }>> {
  return qx.select(
    `SELECT purl, name FROM packages
     WHERE ecosystem = 'go' AND purl > $(after)
     ORDER BY purl ASC
     LIMIT $(limit)`,
    { after: afterPurl, limit: batchSize },
  )
}

export async function enrichGoVersionsBatch(
  afterPurl: string,
  batchSize: number,
): Promise<string | null> {
  const qx = await getPackagesDb()
  const rows = await getGoBatch(qx, afterPurl, batchSize)
  if (rows.length === 0) return null

  const { fetchTimeoutMs, proxyConcurrency } = getGoConfig()

  const enrichOne = async (row: { purl: string; name: string }): Promise<void> => {
    Context.current().heartbeat(row.purl)
    const result = await fetchLatest(row.name, fetchTimeoutMs)
    if (isFetchError(result)) {
      log.warn(
        { purl: row.purl, name: row.name, kind: result.kind, statusCode: result.statusCode },
        'go proxy fetch failed — skipping package',
      )
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
      { version: result.version, releaseAt: result.releaseAt, repoUrl: result.repoUrl, purl: row.purl },
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
  return rows[rows.length - 1].purl
}

export async function enrichGoStatusBatch(
  afterPurl: string,
  batchSize: number,
): Promise<string | null> {
  const qx = await getPackagesDb()
  const rows = await getGoBatch(qx, afterPurl, batchSize)
  if (rows.length === 0) return null

  const { fetchTimeoutMs } = getGoConfig()
  for (const row of rows) {
    Context.current().heartbeat(row.purl)
    const result = await fetchStatus(row.name, fetchTimeoutMs)
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
  return rows[rows.length - 1].purl
}
