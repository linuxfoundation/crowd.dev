import { heartbeat } from '@temporalio/activity'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { prepareBulkInsert } from '@crowd/data-access-layer/src/utils'
import { TinybirdClient } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { canonicalGovernanceRepoUrl } from '../governance/mapRows'
import { findRepoIdsByUrl, readRunStart } from '../governance/syncGovernanceContributors'
import {
  GIT_ACTIVITY_SOURCE,
  GitActivityContributorRow,
  TinybirdCommitContributorRow,
  toGitActivityContributorRow,
} from './mapRows'
import {
  CommitContributorPageKey,
  pageKeyOf,
  readCommitContributorPage,
  readSnapshotComputedAt,
} from './readCommitContributors'

const log = getServiceChildLogger('syncGitActivityContributors')

const PAGE_SIZE = 5000
const WATERMARK_MARGIN_MS = 24 * 60 * 60 * 1000

export interface GitActivitySyncOptions {
  full: boolean
}

export interface GitActivitySyncCounts {
  full: boolean
  read: number
  upserted: number
  removed: number
  skippedUnknownRepo: number
}

export type GitActivityPageCounts = Pick<GitActivitySyncCounts, 'upserted' | 'skippedUnknownRepo'>

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
  'cdp_member_id',
]

const UPSERT_CLAUSE = `(repo_id, source, identity_type, identity_value, role) DO UPDATE SET
  role_kind     = EXCLUDED.role_kind,
  first_seen_at = LEAST(repo_contributors.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at  = GREATEST(repo_contributors.last_seen_at, EXCLUDED.last_seen_at),
  ended_at      = EXCLUDED.ended_at,
  cdp_member_id = EXCLUDED.cdp_member_id,
  updated_at    = NOW()`

export async function readWatermark(pkgsQx: QueryExecutor): Promise<Date | null> {
  const rows: Array<{ watermarkMs: number }> = await pkgsQx.select(
    `SELECT FLOOR(EXTRACT(EPOCH FROM watermark) * 1000) AS "watermarkMs"
     FROM repo_contributors_sync_state WHERE source = $(source)`,
    { source: GIT_ACTIVITY_SOURCE },
  )
  return rows.length === 0 ? null : new Date(rows[0].watermarkMs)
}

export async function writeWatermark(pkgsQx: QueryExecutor, watermark: Date): Promise<void> {
  await pkgsQx.result(
    `INSERT INTO repo_contributors_sync_state (source, watermark, updated_at)
     VALUES ($(source), $(watermark), NOW())
     ON CONFLICT (source) DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = NOW()`,
    { source: GIT_ACTIVITY_SOURCE, watermark },
  )
}

export function resolveUpdatedSince(watermark: Date | null, full: boolean): Date | null {
  if (full || watermark === null) return null
  return new Date(watermark.getTime() - WATERMARK_MARGIN_MS)
}

export function assertSnapshotIsFresh(computedAt: Date | null, watermark: Date | null): Date {
  if (computedAt === null) {
    throw new Error('Tinybird repo_commit_contributors_copy_ds is empty')
  }
  if (watermark !== null && computedAt.getTime() <= watermark.getTime()) {
    throw new Error(
      `Tinybird repo_commit_contributors snapshot (${computedAt.toISOString()}) is not newer than the watermark (${watermark.toISOString()})`,
    )
  }
  return computedAt
}

function contributorRowKey(row: GitActivityContributorRow): string {
  return [row.repoId, row.identityType, row.identityValue].join('\u0000')
}

export function mergeContributorRows(
  rows: GitActivityContributorRow[],
): GitActivityContributorRow[] {
  const byKey = new Map<string, GitActivityContributorRow>()
  for (const row of rows) {
    const key = contributorRowKey(row)
    const existing = byKey.get(key)
    if (!existing) {
      byKey.set(key, row)
      continue
    }
    byKey.set(key, {
      ...existing,
      firstSeenAt: existing.firstSeenAt < row.firstSeenAt ? existing.firstSeenAt : row.firstSeenAt,
      lastSeenAt: existing.lastSeenAt > row.lastSeenAt ? existing.lastSeenAt : row.lastSeenAt,
    })
  }
  return [...byKey.values()]
}

function toDbRow(row: GitActivityContributorRow): Record<string, unknown> {
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
    cdp_member_id: row.cdpMemberId,
  }
}

export async function upsertGitActivityContributors(
  pkgsQx: QueryExecutor,
  rows: GitActivityContributorRow[],
): Promise<number> {
  if (rows.length === 0) return 0
  return pkgsQx.result(
    prepareBulkInsert('repo_contributors', CONTRIBUTOR_COLUMNS, rows.map(toDbRow), UPSERT_CLAUSE),
  )
}

export async function removeGitActivityContributorsUntouchedSince(
  pkgsQx: QueryExecutor,
  runStartedAt: Date,
): Promise<number> {
  return pkgsQx.result(
    'DELETE FROM repo_contributors WHERE source = $(source) AND updated_at < $(runStartedAt)',
    { source: GIT_ACTIVITY_SOURCE, runStartedAt },
  )
}

export async function applyGitActivityPage(
  pkgsQx: QueryExecutor,
  page: TinybirdCommitContributorRow[],
): Promise<GitActivityPageCounts> {
  const canonicalUrls = page.map((row) => canonicalGovernanceRepoUrl(row.channel))
  const knownUrls = canonicalUrls.filter((url): url is string => url !== null)
  const repoIdByUrl = await findRepoIdsByUrl(pkgsQx, [...new Set(knownUrls)])

  const rows: GitActivityContributorRow[] = []
  let skippedUnknownRepo = 0
  page.forEach((row, i) => {
    const url = canonicalUrls[i]
    const repoId = url === null ? undefined : repoIdByUrl.get(url)
    if (repoId === undefined) {
      skippedUnknownRepo += 1
      return
    }
    rows.push(toGitActivityContributorRow(row, repoId))
  })

  const upserted = await upsertGitActivityContributors(pkgsQx, mergeContributorRows(rows))
  return { upserted, skippedUnknownRepo }
}

export async function syncGitActivityContributors(
  options: GitActivitySyncOptions,
): Promise<GitActivitySyncCounts> {
  const pkgsQx = await getPackagesDb()
  const tb = new TinybirdClient()

  const runStartedAt = await readRunStart(pkgsQx)
  const watermark = await readWatermark(pkgsQx)
  const snapshotComputedAt = assertSnapshotIsFresh(await readSnapshotComputedAt(tb), watermark)
  const full = options.full || watermark === null
  const updatedSince = resolveUpdatedSince(watermark, full)

  const counts: GitActivitySyncCounts = {
    full,
    read: 0,
    upserted: 0,
    removed: 0,
    skippedUnknownRepo: 0,
  }

  let after: CommitContributorPageKey | null = null
  for (;;) {
    const page = await readCommitContributorPage(tb, after, updatedSince, PAGE_SIZE)
    if (page.length === 0) break

    const pageCounts = await applyGitActivityPage(pkgsQx, page)
    counts.read += page.length
    counts.upserted += pageCounts.upserted
    counts.skippedUnknownRepo += pageCounts.skippedUnknownRepo

    after = pageKeyOf(page[page.length - 1])
    heartbeat(counts)
  }

  if (counts.read === 0 || counts.upserted === 0) {
    throw new Error(
      `Refusing to finish git-activity contributors sync: read ${counts.read} rows, upserted ${counts.upserted}`,
    )
  }

  if (full) {
    counts.removed = await removeGitActivityContributorsUntouchedSince(pkgsQx, runStartedAt)
  }

  await writeWatermark(pkgsQx, snapshotComputedAt)

  log.info(counts, 'Git-activity contributors synced from Tinybird')
  return counts
}
