import { beforeEach, describe, expect, it, vi } from 'vitest'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { TinybirdClient } from '@crowd/database'

import { getPackagesDb } from '../../db'
import {
  TinybirdCommitContributorRow,
  parseTinybirdDateTime,
  toGitActivityContributorRow,
} from '../git-activity/mapRows'
import { readCommitContributorPage } from '../git-activity/readCommitContributors'
import {
  assertSnapshotIsFresh,
  mergeContributorRows,
  resolveUpdatedSince,
  syncGitActivityContributors,
} from '../git-activity/syncGitActivityContributors'

vi.mock('@temporalio/activity', () => ({ heartbeat: vi.fn() }))
vi.mock('../../db', () => ({ getPackagesDb: vi.fn() }))
vi.mock('@crowd/database', () => ({ TinybirdClient: vi.fn() }))

const runStart = new Date('2026-10-03T06:00:00Z')
const computedAt = '2026-10-03 03:45:00'
const watermark = new Date('2026-10-02T03:45:00Z')

function tbRow(
  overrides: Partial<TinybirdCommitContributorRow> = {},
): TinybirdCommitContributorRow {
  return {
    channel: 'https://github.com/org/repo',
    memberId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    platform: 'github',
    username: 'JaneDoe',
    commitCount: '12',
    firstCommitAt: '2020-01-01 10:00:00.000',
    lastCommitAt: '2026-09-30 12:30:00.000',
    lastUpdatedAt: '2026-10-02 20:00:00.000',
    ...overrides,
  }
}

function stubTinybird(pages: TinybirdCommitContributorRow[][], snapshotComputedAt = computedAt) {
  const executeSql = vi.fn().mockImplementation(async (query: string) => {
    if (query.includes('max(computedAt)')) return { data: [{ computedAt: snapshotComputedAt }] }
    return { data: pages.shift() ?? [] }
  })
  vi.mocked(TinybirdClient).mockImplementation(function () {
    return { executeSql } as unknown as TinybirdClient
  })
  return executeSql
}

function stubPackagesQx(
  repos: Array<{ id: string; url: string }>,
  storedWatermark: Date | null,
  removed = 0,
) {
  const select = vi
    .fn()
    .mockResolvedValueOnce([{ nowMs: runStart.getTime() }])
    .mockResolvedValueOnce(storedWatermark ? [{ watermarkMs: storedWatermark.getTime() }] : [])
    .mockResolvedValue(repos)
  const result = vi
    .fn()
    .mockImplementation((sql: string) => Promise.resolve(sql.startsWith('DELETE') ? removed : 1))
  const qx = { select, result } as unknown as QueryExecutor
  vi.mocked(getPackagesDb).mockResolvedValue(qx)
  return { select, result }
}

function sqlCalls(result: ReturnType<typeof vi.fn>, prefix: string): string[] {
  return result.mock.calls.map((c) => String(c[0])).filter((sql) => sql.startsWith(prefix))
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('parseTinybirdDateTime', () => {
  it('reads Tinybird datetimes as UTC', () => {
    expect(parseTinybirdDateTime('2026-10-03 03:45:00').toISOString()).toBe(
      '2026-10-03T03:45:00.000Z',
    )
    expect(parseTinybirdDateTime('2026-10-03 03:45:00.123').toISOString()).toBe(
      '2026-10-03T03:45:00.123Z',
    )
  })

  it('throws on garbage', () => {
    expect(() => parseTinybirdDateTime('not a date')).toThrow('Unparseable Tinybird datetime')
  })
})

describe('toGitActivityContributorRow', () => {
  it('maps a github login to a lowercase github-login identity with member and count', () => {
    const row = toGitActivityContributorRow(tbRow(), '42')
    expect(row).toMatchObject({
      repoId: '42',
      source: 'git_activity',
      role: 'commit-author',
      roleKind: 'contributor',
      identityType: 'github-login',
      identityValue: 'janedoe',
      endedAt: null,
      cdpMemberId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      commitCount: 12,
    })
    expect(row.firstSeenAt.toISOString()).toBe('2020-01-01T10:00:00.000Z')
    expect(row.lastSeenAt.toISOString()).toBe('2026-09-30T12:30:00.000Z')
  })

  it('maps a git author email to an email identity', () => {
    const row = toGitActivityContributorRow(
      tbRow({ platform: 'git', username: 'Jane@Example.com' }),
      '42',
    )
    expect(row.identityType).toBe('email')
    expect(row.identityValue).toBe('jane@example.com')
  })
})

describe('mergeContributorRows', () => {
  it('sums counts and widens the seen window for rows sharing repo and identity', () => {
    const a = toGitActivityContributorRow(tbRow({ commitCount: 3 }), '1')
    const b = toGitActivityContributorRow(
      tbRow({
        commitCount: 4,
        firstCommitAt: '2019-01-01 00:00:00.000',
        lastCommitAt: '2026-10-01 00:00:00.000',
      }),
      '1',
    )
    const merged = mergeContributorRows([a, b])
    expect(merged).toHaveLength(1)
    expect(merged[0].commitCount).toBe(7)
    expect(merged[0].firstSeenAt).toEqual(b.firstSeenAt)
    expect(merged[0].lastSeenAt).toEqual(b.lastSeenAt)
  })

  it('keeps rows that differ in repo or identity', () => {
    const rows = [
      toGitActivityContributorRow(tbRow(), '1'),
      toGitActivityContributorRow(tbRow(), '2'),
      toGitActivityContributorRow(tbRow({ username: 'other' }), '1'),
    ]
    expect(mergeContributorRows(rows)).toHaveLength(3)
  })
})

describe('resolveUpdatedSince', () => {
  it('is null for full runs and first runs', () => {
    expect(resolveUpdatedSince(null, false)).toBeNull()
    expect(resolveUpdatedSince(watermark, true)).toBeNull()
  })

  it('subtracts a one-day margin from the watermark', () => {
    expect(resolveUpdatedSince(watermark, false)?.toISOString()).toBe('2026-10-01T03:45:00.000Z')
  })
})

describe('assertSnapshotIsFresh', () => {
  it('throws on an empty snapshot', () => {
    expect(() => assertSnapshotIsFresh(null, null)).toThrow('is empty')
  })

  it('throws when the snapshot is not newer than the watermark', () => {
    expect(() => assertSnapshotIsFresh(watermark, watermark)).toThrow('not newer')
  })

  it('returns the snapshot time otherwise', () => {
    const fresh = new Date('2026-10-03T03:45:00Z')
    expect(assertSnapshotIsFresh(fresh, watermark)).toBe(fresh)
    expect(assertSnapshotIsFresh(fresh, null)).toBe(fresh)
  })
})

describe('readCommitContributorPage', () => {
  it('passes the keyset cursor and watermark as template params', async () => {
    const executeSql = vi.fn().mockResolvedValue({ data: [] })
    const tb = { executeSql } as unknown as TinybirdClient
    const after = { channel: 'c', memberId: 'm', platform: 'github', username: 'u' }

    await readCommitContributorPage(tb, after, watermark, 10)

    const [query, params] = executeSql.mock.calls[0]
    expect(query).toContain('{% if defined(afterChannel) %}')
    expect(params).toEqual({
      limit: 10,
      afterChannel: 'c',
      afterMemberId: 'm',
      afterPlatform: 'github',
      afterUsername: 'u',
      updatedSince: '2026-10-02 03:45:00.000',
    })
  })

  it('omits cursor and watermark params on a first full page', async () => {
    const executeSql = vi.fn().mockResolvedValue({ data: [] })
    const tb = { executeSql } as unknown as TinybirdClient

    await readCommitContributorPage(tb, null, null, 10)

    expect(executeSql.mock.calls[0][1]).toEqual({ limit: 10 })
  })
})

describe('syncGitActivityContributors', () => {
  const repo = { id: '1', url: 'https://github.com/org/repo' }

  it('runs a full backfill when no watermark is stored and writes the snapshot time', async () => {
    const executeSql = stubTinybird([[tbRow()], []])
    const { result } = stubPackagesQx([repo], null, 3)

    const counts = await syncGitActivityContributors({ full: false })

    expect(counts).toEqual({ full: true, read: 1, upserted: 1, removed: 3, skippedUnknownRepo: 0 })
    expect(executeSql.mock.calls[1][1]).toEqual({ limit: 5000 })
    expect(sqlCalls(result, 'DELETE')).toHaveLength(1)
    const [watermarkSql, watermarkParams] = result.mock.calls[result.mock.calls.length - 1]
    expect(watermarkSql).toContain('repo_contributors_sync_state')
    expect(watermarkParams.watermark.toISOString()).toBe('2026-10-03T03:45:00.000Z')
  })

  it('runs incrementally from the watermark minus a day and does not delete', async () => {
    const executeSql = stubTinybird([[tbRow()], []])
    const { result } = stubPackagesQx([repo], watermark)

    const counts = await syncGitActivityContributors({ full: false })

    expect(counts.full).toBe(false)
    expect(counts.removed).toBe(0)
    expect(executeSql.mock.calls[1][1]).toMatchObject({ updatedSince: '2026-10-01 03:45:00.000' })
    expect(sqlCalls(result, 'DELETE')).toHaveLength(0)
  })

  it('ignores the watermark and reconciles when full is requested', async () => {
    const executeSql = stubTinybird([[tbRow()], []])
    const { result } = stubPackagesQx([repo], watermark, 2)

    const counts = await syncGitActivityContributors({ full: true })

    expect(counts.full).toBe(true)
    expect(counts.removed).toBe(2)
    expect(executeSql.mock.calls[1][1]).toEqual({ limit: 5000 })
    expect(sqlCalls(result, 'DELETE')).toHaveLength(1)
  })

  it('advances the keyset cursor between pages and skips unknown repos', async () => {
    const executeSql = stubTinybird([
      [tbRow(), tbRow({ channel: 'https://github.com/org/unknown' })],
      [tbRow({ username: 'second' })],
      [],
    ])
    stubPackagesQx([repo], watermark)

    const counts = await syncGitActivityContributors({ full: false })

    expect(counts.read).toBe(3)
    expect(counts.skippedUnknownRepo).toBe(1)
    expect(executeSql.mock.calls[2][1]).toMatchObject({
      afterChannel: 'https://github.com/org/unknown',
      afterUsername: 'JaneDoe',
    })
  })

  it('throws when the snapshot is stale and touches nothing', async () => {
    stubTinybird([[tbRow()], []], '2026-10-02 03:45:00')
    const { result } = stubPackagesQx([repo], watermark)

    await expect(syncGitActivityContributors({ full: false })).rejects.toThrow('not newer')
    expect(result).not.toHaveBeenCalled()
  })

  it('throws when nothing was read and leaves the watermark untouched', async () => {
    stubTinybird([[]])
    const { result } = stubPackagesQx([repo], watermark)

    await expect(syncGitActivityContributors({ full: true })).rejects.toThrow('read 0 rows')
    expect(result).not.toHaveBeenCalled()
  })

  it('throws when rows were read but none upserted', async () => {
    stubTinybird([[tbRow({ channel: 'https://github.com/org/unknown' })], []])
    const { result } = stubPackagesQx([repo], watermark)

    await expect(syncGitActivityContributors({ full: true })).rejects.toThrow('upserted 0')
    expect(sqlCalls(result, 'INSERT INTO repo_contributors_sync_state')).toHaveLength(0)
  })
})
