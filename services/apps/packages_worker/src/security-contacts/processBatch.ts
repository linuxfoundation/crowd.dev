import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'
import { mapWithConcurrency } from '../utils/concurrency'

import { extractPvr } from './extractors/pvr'
import { extractManifest } from './extractors/registry'
import { extractSecurityContactsFile } from './extractors/securityContactsFile'
import { extractSecurityInsights } from './extractors/securityInsights'
import { extractSecurityMd } from './extractors/securityMd'
import { extractSecurityTxt } from './extractors/securityTxt'
import { getSecurityContactsToken } from './githubToken'
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
//  - CONCURRENCY: parallel repos; safe against GitHub REST secondary limits given the token pool.
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
  packages: RepoPackage[] | null
}

async function fetchBatch(qx: QueryExecutor): Promise<SweepRow[]> {
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
        -- never evaluated → always eligible
        r.contacts_last_refreshed IS NULL
        -- evaluated but no contacts found yet → retry on the daily cadence
        OR (
          NOT EXISTS (SELECT 1 FROM security_contacts sc WHERE sc.repo_id = r.id)
          AND r.contacts_last_refreshed < NOW() - INTERVAL '$(dailyIntervalHours) hours'
        )
        -- already enriched (has contacts) → refresh on the weekly cadence
        OR (
          EXISTS (SELECT 1 FROM security_contacts sc WHERE sc.repo_id = r.id)
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
  return { repoId: row.id, url: row.url, homepage: row.homepage, packages: row.packages ?? [] }
}

async function processRepo(
  target: RepoTarget,
  deps: ExtractorDeps,
  qx: QueryExecutor,
): Promise<void> {
  // One failing extractor must not sink the repo.
  const results = await Promise.allSettled(EXTRACTORS.map((extract) => extract(target, deps)))

  // Every extractor failed (transient outage) — preserve existing data, just record the attempt.
  if (results.every((r) => r.status === 'rejected')) {
    log.warn({ repoId: target.repoId }, 'All extractors failed — preserving existing data')
    await markRepoAttempted(qx, target.repoId)
    return
  }

  let contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  for (const r of results) {
    if (r.status !== 'fulfilled') {
      log.warn({ repoId: target.repoId, errMsg: r.reason?.message }, 'Extractor failed')
      continue
    }
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

  const deps: ExtractorDeps = {
    fetchTimeoutMs: FETCH_TIMEOUT_MS,
    userAgent: config.userAgent,
    getToken: getSecurityContactsToken,
  }

  const targets = batch.map(toTarget)
  await mapWithConcurrency(targets, CONCURRENCY, (target) => processRepo(target, deps, qx))

  log.info({ processed: targets.length }, 'Security contacts batch complete')
  return { processed: targets.length }
}
