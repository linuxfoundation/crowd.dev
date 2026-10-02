import { heartbeat } from '@temporalio/activity'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { prepareBulkInsert } from '@crowd/data-access-layer/src/utils'
import { getServiceChildLogger } from '@crowd/logging'

import { getCdpDb, getPackagesDb } from '../../db'
import {
  CdpGovernanceRoleRow,
  GOVERNANCE_FILE_SOURCE,
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
  removed: number
  skippedUnknownRepo: number
}

export type GovernancePageCounts = Omit<GovernanceSyncCounts, 'read' | 'removed'>

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

function contributorUpsertClause(runStartedAt: Date): string {
  const runStart = `'${runStartedAt.toISOString()}'::timestamptz`
  return `(repo_id, source, identity_type, identity_value, role) DO UPDATE SET
  role_kind     = EXCLUDED.role_kind,
  first_seen_at = EXCLUDED.first_seen_at,
  last_seen_at  = EXCLUDED.last_seen_at,
  ended_at      = EXCLUDED.ended_at,
  updated_at    = NOW()
  WHERE NOT (repo_contributors.updated_at >= ${runStart}
    AND repo_contributors.ended_at IS NULL
    AND EXCLUDED.ended_at IS NOT NULL)`
}

export async function readRunStart(pkgsQx: QueryExecutor): Promise<Date> {
  const rows: Array<{ nowMs: number }> = await pkgsQx.select(
    'SELECT FLOOR(EXTRACT(EPOCH FROM NOW()) * 1000) AS "nowMs"',
  )
  return new Date(rows[0].nowMs)
}

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
  runStartedAt: Date,
): Promise<number> {
  if (rows.length === 0) return 0
  return pkgsQx.result(
    prepareBulkInsert(
      'repo_contributors',
      CONTRIBUTOR_COLUMNS,
      rows.map(toDbRow),
      contributorUpsertClause(runStartedAt),
    ),
  )
}

export async function removeContributorsUntouchedSince(
  pkgsQx: QueryExecutor,
  runStartedAt: Date,
): Promise<number> {
  return pkgsQx.result(
    'DELETE FROM repo_contributors WHERE source = $(source) AND updated_at < $(runStartedAt)',
    { source: GOVERNANCE_FILE_SOURCE, runStartedAt },
  )
}

export async function applyGovernancePage(
  pkgsQx: QueryExecutor,
  page: CdpGovernanceRoleRow[],
  runStartedAt: Date,
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
    rows.push(toRepoContributorRow(row, repoId, runStartedAt))
  })

  const deduped = dedupeContributorRows(rows)
  const upserted = await upsertRepoContributors(pkgsQx, deduped, runStartedAt)

  return {
    upserted,
    ended: deduped.filter((row) => row.endedAt !== null).length,
    skippedUnknownRepo,
  }
}

export async function syncGovernanceContributors(): Promise<GovernanceSyncCounts> {
  const cdpQx = await getCdpDb()
  const pkgsQx = await getPackagesDb()
  const runStartedAt = await readRunStart(pkgsQx)
  const counts: GovernanceSyncCounts = {
    read: 0,
    upserted: 0,
    ended: 0,
    removed: 0,
    skippedUnknownRepo: 0,
  }

  let afterId = FIRST_UUID
  for (;;) {
    const page = await readGovernanceRolePage(cdpQx, afterId, PAGE_SIZE)
    if (page.length === 0) break

    const pageCounts = await applyGovernancePage(pkgsQx, page, runStartedAt)
    counts.read += page.length
    counts.upserted += pageCounts.upserted
    counts.ended += pageCounts.ended
    counts.skippedUnknownRepo += pageCounts.skippedUnknownRepo

    afterId = page[page.length - 1].id
    heartbeat(counts)
  }

  if (counts.read === 0 || counts.upserted === 0) {
    throw new Error(
      `Refusing to reconcile repo_contributors: read ${counts.read} governance-file roles, upserted ${counts.upserted}`,
    )
  }

  counts.removed = await removeContributorsUntouchedSince(pkgsQx, runStartedAt)

  log.info(counts, 'Governance-file contributors synced from CDP')
  return counts
}
