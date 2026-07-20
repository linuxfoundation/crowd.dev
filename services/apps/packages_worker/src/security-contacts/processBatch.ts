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
import { deriveGithubHandlesFromNoreplyEmails, resolveCdpEmails } from './resolveCdpEmails'
import {
  Extractor,
  ExtractorDeps,
  ProcessRepoResult,
  RawContact,
  RepoPackage,
  RepoPolicies,
  RepoTarget,
} from './types'
import { verifyHandleCandidates } from './verifyHandleCandidates'
import { writeContactsBatch } from './writeContacts'

const log = getServiceChildLogger('security-contacts')

type Config = ReturnType<typeof getSecurityContactsConfig>

export interface BatchResult {
  /** Repos evaluated in this batch. 0 signals the workflow there is no more work. */
  processed: number
}

// Just under 24h/168h so a repo processed at ~06:00 is still eligible at the next tick.
const DAILY_INTERVAL_HOURS = 20 // no contacts found yet
const WEEKLY_INTERVAL_HOURS = 156 // already has contacts

// GitHub calls are rate-limit-aware (see githubToken.ts), so high repo concurrency is safe.
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
    WHERE (
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

export function buildBaseDeps(config: Config): Omit<ExtractorDeps, 'repoTree'> {
  return {
    fetchTimeoutMs: FETCH_TIMEOUT_MS,
    userAgent: config.userAgent,
    githubGet: (path, opts) => githubApiGet(path, FETCH_TIMEOUT_MS, opts),
  }
}

export async function processRepo(
  target: RepoTarget,
  baseDeps: Omit<ExtractorDeps, 'repoTree'>,
  cdpQx: QueryExecutor,
): Promise<ProcessRepoResult> {
  // One tree fetch per repo, shared by extractors that probe well-known paths.
  let repoTree: ExtractorDeps['repoTree'] = { paths: null }
  try {
    const { owner, name } = parseGithubUrl(target.url)
    repoTree = await fetchRepoTree(owner, name, baseDeps.githubGet)
  } catch {
    // not a github.com URL
  }
  const deps: ExtractorDeps = { ...baseDeps, repoTree }

  const results = await Promise.allSettled(EXTRACTORS.map((extract) => extract(target, deps)))

  let failedCount = 0
  results.forEach((r, i) => {
    if (r.status !== 'rejected') return
    failedCount++
    log.warn(
      { repoId: target.repoId, extractor: EXTRACTORS[i].name, errMsg: r.reason?.message },
      'Extractor failed — keeping contacts from remaining sources',
    )
  })
  if (failedCount === results.length) {
    return { repoId: target.repoId, status: 'extractor-failed' }
  }

  let contacts: RawContact[] = []
  const policies: Partial<RepoPolicies> = {}
  const handleCandidates: RawContact[] = []
  for (const r of results) {
    if (r.status !== 'fulfilled') continue
    contacts.push(...r.value.contacts)
    handleCandidates.push(...(r.value.handleCandidates ?? []))
    for (const [key, value] of Object.entries(r.value.policies)) {
      if (!(policies as Record<string, unknown>)[key] && value != null) {
        ;(policies as Record<string, unknown>)[key] = value
      }
    }
  }

  // A2 veto: drop B1's github-pvr guess when A2 authoritatively reports PVR disabled.
  if (policies.pvrEnabled === false) {
    contacts = contacts.filter((c) => c.channel !== 'github-pvr')
  }

  contacts.push(...(await verifyHandleCandidates(target, deps, handleCandidates)))

  contacts.push(...deriveGithubHandlesFromNoreplyEmails(contacts))

  const handleContacts = contacts.filter((c) => c.channel === 'github-handle')
  if (handleContacts.length > 0) {
    try {
      contacts.push(...(await resolveCdpEmails(cdpQx, handleContacts)))
    } catch (err) {
      log.warn(
        { repoId: target.repoId, errMsg: (err as Error).message },
        'CDP email resolution failed — proceeding without resolved emails',
      )
    }
  }

  const scored = reconcile(contacts)
  return {
    repoId: target.repoId,
    status: failedCount > 0 ? 'partial' : 'ok',
    contacts: scored,
    policies,
  }
}

export async function processBatch(
  qx: QueryExecutor,
  cdpQx: QueryExecutor,
  config: Config,
): Promise<BatchResult> {
  const batch = await fetchBatch(qx)
  if (batch.length === 0) return { processed: 0 }

  const deps = buildBaseDeps(config)

  const targets = batch.map(toTarget)
  // Fixed-cadence heartbeat: a slow repo can outlast the 2-minute heartbeatTimeout even while
  // every concurrency slot is still busy, so this can't rely on task completions alone.
  const heartbeatTimer = setInterval(() => {
    try {
      heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'Heartbeat failed')
    }
  }, 30_000)
  let outcomes: ProcessRepoResult[]
  const extractionStartedAt = Date.now()
  try {
    // A cancelled task (superseded by a newer activity attempt) is left to throw so it stops
    // scheduling further repos instead of racing the new attempt.
    //
    // Extraction is collected here and persisted afterward in one batched call (below) rather
    // than per-repo, since per-repo writes meant up to CONCURRENCY concurrent transactions
    // against a packages-db pool sized for far fewer connections — the actual sweep bottleneck.
    outcomes = await mapWithConcurrency(targets, CONCURRENCY, async (target) => {
      if (cancellationSignal().aborted) {
        throw new Error(
          'Security contacts batch cancelled — superseded by a newer activity attempt',
        )
      }
      try {
        return await processRepo(target, deps, cdpQx)
      } catch (err) {
        log.error(
          { repoId: target.repoId, errMsg: (err as Error).message },
          'Repo processing failed',
        )
        return { repoId: target.repoId, status: 'extractor-failed' as const }
      }
    })

    const extractionDurationMs = Date.now() - extractionStartedAt
    const ok = outcomes.filter((o) => o.status === 'ok').length
    const partial = outcomes.filter((o) => o.status === 'partial').length
    log.info(
      {
        ok,
        partial,
        failed: outcomes.length - ok - partial,
        durationMs: extractionDurationMs,
        reposPerSec: Number((outcomes.length / (extractionDurationMs / 1000)).toFixed(1)),
      },
      'Security contacts batch extraction complete — persisting',
    )

    // Heartbeat is kept alive through persistence too: a slow/lock-blocked write can outlast the
    // 2-minute heartbeat timeout just like extraction can, and letting the timer stop early would
    // let Temporal retry this activity while the original attempt is still writing.
    const writeStartedAt = Date.now()
    await writeContactsBatch(qx, outcomes)
    const writeDurationMs = Date.now() - writeStartedAt
    log.info(
      {
        durationMs: writeDurationMs,
        reposPerSec: Number((outcomes.length / (writeDurationMs / 1000)).toFixed(1)),
      },
      'Security contacts batch persistence complete',
    )
  } finally {
    clearInterval(heartbeatTimer)
  }

  log.info({ processed: targets.length }, 'Security contacts batch complete')
  return { processed: targets.length }
}
