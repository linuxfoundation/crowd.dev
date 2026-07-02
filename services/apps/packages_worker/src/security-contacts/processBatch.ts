import { cancellationSignal, heartbeat } from '@temporalio/activity'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'
import { parseGithubUrl } from '../enricher/fetchLightRepo'
import { mapWithConcurrency } from '../utils/concurrency'

import { fetchRepoTree } from './extractors/gitTree'
import { extractPvr } from './extractors/pvr'
import { extractManifest } from './extractors/registry'
import { extractSecurityContactsFile } from './extractors/securityContactsFile'
import { extractSecurityInsights } from './extractors/securityInsights'
import { extractSecurityMd } from './extractors/securityMd'
import { extractSecurityTxt } from './extractors/securityTxt'
import { githubApiGet } from './githubToken'
import { reconcile } from './reconcile'
import {
  Extractor,
  ExtractorDeps,
  RawContact,
  RepoPackage,
  RepoPolicies,
  RepoTarget,
} from './types'
import { markRepoAttempted, writeContacts } from './writeContacts'

const log = getServiceChildLogger('security-contacts')

type Config = ReturnType<typeof getSecurityContactsConfig>

export interface BatchResult {
  /** Repos evaluated in this batch. 0 signals the workflow there is no more work. */
  processed: number
}

// Two-tier refresh cadence (the worker runs on a daily cron). Just under 24h/168h so a repo
// processed at ~06:00 is eligible again at the next daily/weekly tick rather than slipping a day.
const DAILY_INTERVAL_HOURS = 20 // repos never evaluated or with no contacts yet
const WEEKLY_INTERVAL_HOURS = 156 // already-enriched repos (have contacts)

// Tuned for throughput within the platform ceilings:
//  - CONCURRENCY: parallel repos. GitHub calls go through the authed Contents API via a
//    rate-limit-aware pool (per-installation budget parking + app-wide concurrency gate +
//    Retry-After backoff in githubToken), so high repo concurrency won't trip GitHub limits.
//  - FETCH_TIMEOUT_MS: generous enough for slow registries (Maven metadata/POM) without hanging slots.
//  - BATCH_SIZE: bounded by the 30-min activity timeout (worst case: an all-cargo batch throttled
//    to crates.io's 1 req/s finishes ~8 min).
const CONCURRENCY = 100
const FETCH_TIMEOUT_MS = 15000
const BATCH_SIZE = 500

const EXTRACTORS: Extractor[] = [
  extractSecurityInsights, // A1
  extractPvr, // A2
  extractSecurityContactsFile, // A3
  extractSecurityTxt, // A4
  extractSecurityMd, // B1
  extractManifest, // B2
]

interface SweepRow {
  id: string
  url: string
  homepage: string | null
  archived: boolean | null
  packages: RepoPackage[] | null
}

async function fetchBatch(qx: QueryExecutor): Promise<SweepRow[]> {
  return qx.select(
    `
    SELECT r.id::text AS id,
           r.url,
           r.homepage,
           r.archived,
           json_agg(json_build_object('purl', p.purl, 'ecosystem', p.ecosystem)) AS packages
    FROM repos r
    JOIN package_repos pr ON pr.repo_id = r.id
    JOIN packages p ON p.id = pr.package_id AND p.is_critical
    WHERE r.host = 'github'
      AND (
        -- never evaluated → always eligible
        r.contacts_last_refreshed IS NULL
        -- evaluated but no contacts found yet → retry on the daily cadence
        OR (
          NOT EXISTS (SELECT 1 FROM security_contacts sc WHERE sc.repo_id = r.id AND sc.deleted_at IS NULL)
          AND r.contacts_last_refreshed < NOW() - INTERVAL '$(dailyIntervalHours) hours'
        )
        -- already enriched (has contacts) → refresh on the weekly cadence
        OR (
          EXISTS (SELECT 1 FROM security_contacts sc WHERE sc.repo_id = r.id AND sc.deleted_at IS NULL)
          AND r.contacts_last_refreshed < NOW() - INTERVAL '$(weeklyIntervalHours) hours'
        )
      )
    GROUP BY r.id
    ORDER BY r.id
    LIMIT $(batchSize)
    `,
    {
      batchSize: BATCH_SIZE,
      dailyIntervalHours: DAILY_INTERVAL_HOURS,
      weeklyIntervalHours: WEEKLY_INTERVAL_HOURS,
    },
  )
}

function toTarget(row: SweepRow): RepoTarget {
  return {
    repoId: row.id,
    url: row.url,
    homepage: row.homepage,
    archived: row.archived,
    packages: row.packages ?? [],
  }
}

async function processRepo(
  target: RepoTarget,
  baseDeps: Omit<ExtractorDeps, 'repoTree'>,
  qx: QueryExecutor,
): Promise<void> {
  // One tree fetch per repo, shared by extractors that probe well-known paths (see gitTree.ts).
  let repoTree: ExtractorDeps['repoTree'] = { paths: null }
  try {
    const { owner, name } = parseGithubUrl(target.url)
    repoTree = await fetchRepoTree(owner, name, baseDeps.githubGet)
  } catch {
    // not a github.com URL
  }
  const deps: ExtractorDeps = { ...baseDeps, repoTree }

  const results = await Promise.allSettled(EXTRACTORS.map((extract) => extract(target, deps)))

  // Replace the repo's contacts only when every extractor succeeded. If any failed (transient
  // error — non-200s throw), a destructive rewrite would drop contacts a failed tier-A/B extractor
  // still has, so preserve existing data and just record the attempt; retried next cadence.
  const failed = results.find((r) => r.status === 'rejected') as PromiseRejectedResult | undefined
  if (failed) {
    log.warn(
      { repoId: target.repoId, errMsg: failed.reason?.message },
      'Extractor failed — preserving existing data',
    )
    await markRepoAttempted(qx, target.repoId)
    return
  }

  let contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  for (const r of results) {
    if (r.status !== 'fulfilled') continue
    contacts.push(...r.value.contacts)
    for (const [key, value] of Object.entries(r.value.policies)) {
      if (!(policies as Record<string, unknown>)[key] && value != null) {
        ;(policies as Record<string, unknown>)[key] = value
      }
    }
  }

  // A2 veto: B1 may emit a github-pvr contact from redirect language; drop it when A2
  // authoritatively reports PVR disabled (Option C from the design discussion).
  if (policies.pvrEnabled === false) {
    contacts = contacts.filter((c) => c.channel !== 'github-pvr')
  }

  const scored = reconcile(contacts)
  await writeContacts(qx, target.repoId, scored, policies)
}

export async function processBatch(qx: QueryExecutor, config: Config): Promise<BatchResult> {
  const batch = await fetchBatch(qx)
  if (batch.length === 0) return { processed: 0 }

  const deps: Omit<ExtractorDeps, 'repoTree'> = {
    fetchTimeoutMs: FETCH_TIMEOUT_MS,
    userAgent: config.userAgent,
    githubGet: (path, opts) => githubApiGet(path, FETCH_TIMEOUT_MS, opts),
  }

  const targets = batch.map(toTarget)
  // mapWithConcurrency is fail-fast: a per-repo DB error is caught below so it doesn't abort the
  // batch, but a cancelled task (superseded by a newer activity attempt, see workflows.ts) is
  // left to throw so it stops scheduling further repos instead of racing the new attempt.
  await mapWithConcurrency(targets, CONCURRENCY, async (target) => {
    if (cancellationSignal().aborted) {
      throw new Error('Security contacts batch cancelled — superseded by a newer activity attempt')
    }
    try {
      await processRepo(target, deps, qx)
    } catch (err) {
      log.error({ repoId: target.repoId, errMsg: (err as Error).message }, 'Repo processing failed')
      // Best-effort mark so a persistently-failing repo drains on cadence rather than making the
      // sweep hot-loop (it would otherwise stay eligible and keep processed > 0 forever).
      await markRepoAttempted(qx, target.repoId).catch(() => undefined)
    } finally {
      heartbeat()
    }
  })

  log.info({ processed: targets.length }, 'Security contacts batch complete')
  return { processed: targets.length }
}
