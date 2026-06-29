import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'

import { RepoPackage, RepoTarget } from './types'

const log = getServiceChildLogger('security-contacts')

type Config = ReturnType<typeof getSecurityContactsConfig>

export interface BatchResult {
  /** Repos evaluated in this batch. 0 signals the workflow there is no more work. */
  processed: number
}

interface SweepRow {
  id: string
  url: string
  homepage: string | null
  packages: RepoPackage[] | null
}

/**
 * One page of github repos linked to an is_critical package whose contacts are
 * missing or stale. Progress is driven by repos.contacts_last_refreshed (bumped
 * once a repo is evaluated), so each invocation naturally advances to the next
 * unprocessed repos — no cross-invocation cursor needed.
 */
async function fetchBatch(qx: QueryExecutor, config: Config): Promise<SweepRow[]> {
  return qx.select(
    `
    SELECT r.id::text AS id,
           r.url,
           r.homepage,
           json_agg(json_build_object('purl', p.purl, 'ecosystem', p.ecosystem)) AS packages
    FROM repos r
    JOIN package_repos pr ON pr.repo_id = r.id
    JOIN packages p ON p.id = pr.package_id AND p.is_critical
    WHERE r.host = 'github'
      AND (
        r.contacts_last_refreshed IS NULL
        OR r.contacts_last_refreshed < NOW() - INTERVAL '$(updateIntervalHours) hours'
      )
    GROUP BY r.id
    ORDER BY r.id
    LIMIT $(batchSize)
    `,
    { batchSize: config.batchSize, updateIntervalHours: config.updateIntervalHours },
  )
}

function toTarget(row: SweepRow): RepoTarget {
  return {
    repoId: row.id,
    url: row.url,
    homepage: row.homepage,
    packages: row.packages ?? [],
  }
}

async function runWithConcurrency<T>(
  items: T[],
  concurrency: number,
  task: (item: T) => Promise<void>,
): Promise<void> {
  let idx = 0
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (idx < items.length) {
        const item = items[idx++]
        await task(item)
      }
    }),
  )
}

/**
 * Step 2 placeholder pipeline: builds the RepoTarget and logs it. Extractors,
 * reconcile, score and write are wired in later steps; until then we bump
 * contacts_last_refreshed so the sweep makes progress and the workflow can drain.
 */
async function processRepo(target: RepoTarget): Promise<void> {
  log.trace(
    { repoId: target.repoId, url: target.url, packages: target.packages.length },
    'Evaluated repo (pipeline not yet wired)',
  )
}

/** Marks repos as evaluated. In Step 6 this moves into the per-repo write tx. */
async function markRefreshed(qx: QueryExecutor, repoIds: string[]): Promise<void> {
  if (repoIds.length === 0) return
  await qx.result(
    `UPDATE repos SET contacts_last_refreshed = NOW() WHERE id = ANY($(repoIds)::bigint[])`,
    { repoIds },
  )
}

export async function processBatch(qx: QueryExecutor, config: Config): Promise<BatchResult> {
  const batch = await fetchBatch(qx, config)
  if (batch.length === 0) return { processed: 0 }

  const targets = batch.map(toTarget)
  await runWithConcurrency(targets, config.concurrency, processRepo)
  await markRefreshed(
    qx,
    targets.map((t) => t.repoId),
  )

  log.info({ processed: targets.length }, 'Security contacts batch complete')
  return { processed: targets.length }
}
