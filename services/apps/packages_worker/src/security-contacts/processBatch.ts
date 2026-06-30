import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getSecurityContactsConfig } from '../config'

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
import { writeContacts } from './writeContacts'

const log = getServiceChildLogger('security-contacts')

type Config = ReturnType<typeof getSecurityContactsConfig>

export interface BatchResult {
  /** Repos evaluated in this batch. 0 signals the workflow there is no more work. */
  processed: number
}

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
  return { repoId: row.id, url: row.url, homepage: row.homepage, packages: row.packages ?? [] }
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

async function processRepo(
  target: RepoTarget,
  deps: ExtractorDeps,
  qx: QueryExecutor,
): Promise<void> {
  // One failing extractor must not sink the repo.
  const results = await Promise.allSettled(EXTRACTORS.map((extract) => extract(target, deps)))

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
  const batch = await fetchBatch(qx, config)
  if (batch.length === 0) return { processed: 0 }

  const deps: ExtractorDeps = {
    fetchTimeoutMs: config.fetchTimeoutMs,
    userAgent: config.userAgent,
    getToken: getSecurityContactsToken,
  }

  const targets = batch.map(toTarget)
  await runWithConcurrency(targets, config.concurrency, (target) => processRepo(target, deps, qx))

  log.info({ processed: targets.length }, 'Security contacts batch complete')
  return { processed: targets.length }
}
