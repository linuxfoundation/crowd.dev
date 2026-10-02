import { TinybirdClient } from '@crowd/database'

import { TinybirdCommitContributorRow, parseTinybirdDateTime } from './mapRows'

const DATASOURCE = 'repo_commit_contributors_copy_ds'

interface TinybirdResult<T> {
  data: T[]
}

export type CommitContributorPageKey = Pick<
  TinybirdCommitContributorRow,
  'channel' | 'memberId' | 'platform' | 'username'
>

export function pageKeyOf(row: TinybirdCommitContributorRow): CommitContributorPageKey {
  return {
    channel: row.channel,
    memberId: row.memberId,
    platform: row.platform,
    username: row.username,
  }
}

export function formatTinybirdDateTime(value: Date): string {
  return value.toISOString().replace('T', ' ').replace('Z', '')
}

export async function readSnapshotComputedAt(tb: TinybirdClient): Promise<Date | null> {
  const result = await tb.executeSql<TinybirdResult<{ computedAt: string | null }>>(
    `SELECT toString(max(computedAt)) AS computedAt FROM ${DATASOURCE} FORMAT JSON`,
  )
  const computedAt = result.data[0]?.computedAt
  if (!computedAt || computedAt === '1970-01-01 00:00:00') return null
  return parseTinybirdDateTime(computedAt)
}

export async function readCommitContributorPage(
  tb: TinybirdClient,
  after: CommitContributorPageKey | null,
  updatedSince: Date | null,
  limit: number,
): Promise<TinybirdCommitContributorRow[]> {
  const query = `%
    SELECT channel, memberId, platform, username, commitCount,
           toString(firstCommitAt) AS firstCommitAt,
           toString(lastCommitAt) AS lastCommitAt,
           toString(lastUpdatedAt) AS lastUpdatedAt
    FROM ${DATASOURCE}
    WHERE 1 = 1
    {% if defined(afterChannel) %}
      AND (channel, memberId, platform, username) >
          ({{String(afterChannel)}}, {{String(afterMemberId)}}, {{String(afterPlatform)}}, {{String(afterUsername)}})
    {% end %}
    {% if defined(updatedSince) %}
      AND lastUpdatedAt > parseDateTime64BestEffort({{String(updatedSince)}})
    {% end %}
    ORDER BY channel, memberId, platform, username
    LIMIT {{Int32(limit)}}
    FORMAT JSON`

  const params: Record<string, unknown> = { limit }
  if (after) {
    params.afterChannel = after.channel
    params.afterMemberId = after.memberId
    params.afterPlatform = after.platform
    params.afterUsername = after.username
  }
  if (updatedSince) params.updatedSince = formatTinybirdDateTime(updatedSince)

  const result = await tb.executeSql<TinybirdResult<TinybirdCommitContributorRow>>(query, params)
  return result.data
}
