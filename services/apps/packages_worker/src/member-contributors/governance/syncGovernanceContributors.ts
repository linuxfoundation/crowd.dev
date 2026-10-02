import { heartbeat } from '@temporalio/activity'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { prepareBulkInsert } from '@crowd/data-access-layer/src/utils'
import { getServiceChildLogger } from '@crowd/logging'

import { getCdpDb, getPackagesDb } from '../../db'
import {
  CdpGovernanceRoleRow,
  RepoContributorRow,
  canonicalGovernanceRepoUrl,
  toRepoContributorRow,
} from './mapRows'

const log = getServiceChildLogger('syncGovernanceContributors')

const PAGE_SIZE = 5000
const FIRST_UUID = '00000000-0000-0000-0000-000000000000'

export interface GovernanceSyncCounts {
  read: number
  upserted: number
  ended: number
  skippedUnknownRepo: number
}

export type GovernancePageCounts = Omit<GovernanceSyncCounts, 'read'>

const CONTRIBUTOR_COLUMNS = [
  'repo_id',
  'source',
  'role',
  'role_kind',
  'identity_type',
  'identity_value',
  'first_seen_at',
  'last_seen_at',
  'ended_at',
]

const CONTRIBUTOR_UPSERT_SET = `(repo_id, source, identity_type, identity_value, role) DO UPDATE SET
  role_kind     = EXCLUDED.role_kind,
  first_seen_at = EXCLUDED.first_seen_at,
  last_seen_at  = EXCLUDED.last_seen_at,
  ended_at      = EXCLUDED.ended_at,
  updated_at    = NOW()`

export async function readGovernanceRolePage(
  cdpQx: QueryExecutor,
  afterId: string,
  limit: number,
): Promise<CdpGovernanceRoleRow[]> {
  return cdpQx.select(
    `
    SELECT mi.id, mi."repoUrl", mi.role, mi."originalRole", mi."startDate", mi."endDate", mi."createdAt",
           mem.platform AS "identityPlatform", mem.type AS "identityType", mem.value AS "identityValue"
    FROM public."maintainersInternal" mi
    JOIN public."memberIdentities" mem ON mem.id = mi."identityId" AND mem."deletedAt" IS NULL
    WHERE mi.id > $(afterId)::uuid
    ORDER BY mi.id
    LIMIT $(limit)
    `,
    { afterId, limit },
  )
}

export async function findRepoIdsByUrl(
  pkgsQx: QueryExecutor,
  urls: string[],
): Promise<Map<string, string>> {
  if (urls.length === 0) return new Map()
  const rows: Array<{ id: string; url: string }> = await pkgsQx.select(
    'SELECT id, url FROM repos WHERE url = ANY($(urls)::text[])',
    { urls },
  )
  return new Map(rows.map((r) => [r.url, String(r.id)]))
}

function contributorRowKey(row: RepoContributorRow): string {
  return [row.repoId, row.source, row.identityType, row.identityValue, row.role].join('\u0000')
}

export function dedupeContributorRows(rows: RepoContributorRow[]): RepoContributorRow[] {
  const byKey = new Map<string, RepoContributorRow>()
  for (const row of rows) {
    const key = contributorRowKey(row)
    const existing = byKey.get(key)
    if (!existing || (existing.endedAt !== null && row.endedAt === null)) byKey.set(key, row)
  }
  return [...byKey.values()]
}

function toDbRow(row: RepoContributorRow): Record<string, unknown> {
  return {
    repo_id: row.repoId,
    source: row.source,
    role: row.role,
    role_kind: row.roleKind,
    identity_type: row.identityType,
    identity_value: row.identityValue,
    first_seen_at: row.firstSeenAt,
    last_seen_at: row.lastSeenAt,
    ended_at: row.endedAt,
  }
}

export async function upsertRepoContributors(
  pkgsQx: QueryExecutor,
  rows: RepoContributorRow[],
): Promise<void> {
  if (rows.length === 0) return
  await pkgsQx.result(
    prepareBulkInsert(
      'repo_contributors',
      CONTRIBUTOR_COLUMNS,
      rows.map(toDbRow),
      CONTRIBUTOR_UPSERT_SET,
    ),
  )
}

export async function applyGovernancePage(
  pkgsQx: QueryExecutor,
  page: CdpGovernanceRoleRow[],
  syncedAt: Date,
): Promise<GovernancePageCounts> {
  const canonicalUrls = page.map((row) => canonicalGovernanceRepoUrl(row.repoUrl))
  const knownUrls = canonicalUrls.filter((url): url is string => url !== null)
  const repoIdByUrl = await findRepoIdsByUrl(pkgsQx, [...new Set(knownUrls)])

  const rows: RepoContributorRow[] = []
  let skippedUnknownRepo = 0
  page.forEach((row, i) => {
    const url = canonicalUrls[i]
    const repoId = url === null ? undefined : repoIdByUrl.get(url)
    if (repoId === undefined) {
      skippedUnknownRepo += 1
      return
    }
    rows.push(toRepoContributorRow(row, repoId, syncedAt))
  })

  const deduped = dedupeContributorRows(rows)
  await upsertRepoContributors(pkgsQx, deduped)

  return {
    upserted: deduped.length,
    ended: deduped.filter((row) => row.endedAt !== null).length,
    skippedUnknownRepo,
  }
}

export async function syncGovernanceContributors(): Promise<GovernanceSyncCounts> {
  const cdpQx = await getCdpDb()
  const pkgsQx = await getPackagesDb()
  const syncedAt = new Date()
  const counts: GovernanceSyncCounts = { read: 0, upserted: 0, ended: 0, skippedUnknownRepo: 0 }

  let afterId = FIRST_UUID
  for (;;) {
    const page = await readGovernanceRolePage(cdpQx, afterId, PAGE_SIZE)
    if (page.length === 0) break

    const pageCounts = await applyGovernancePage(pkgsQx, page, syncedAt)
    counts.read += page.length
    counts.upserted += pageCounts.upserted
    counts.ended += pageCounts.ended
    counts.skippedUnknownRepo += pageCounts.skippedUnknownRepo

    afterId = page[page.length - 1].id
    heartbeat(counts)
  }

  log.info(counts, 'Governance-file contributors synced from CDP')
  return counts
}
