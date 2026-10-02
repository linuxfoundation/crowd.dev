import { beforeEach, describe, expect, it, vi } from 'vitest'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { getCdpDb, getPackagesDb } from '../../db'
import { CdpGovernanceRoleRow, RepoContributorRow } from '../governance/mapRows'
import {
  applyGovernancePage,
  dedupeContributorRows,
  syncGovernanceContributors,
} from '../governance/syncGovernanceContributors'

vi.mock('@temporalio/activity', () => ({ heartbeat: vi.fn() }))
vi.mock('../../db', () => ({ getCdpDb: vi.fn(), getPackagesDb: vi.fn() }))

const syncedAt = new Date('2026-10-02T06:00:00Z')

function cdpRow(overrides: Partial<CdpGovernanceRoleRow> = {}): CdpGovernanceRoleRow {
  return {
    id: '11111111-1111-1111-1111-111111111111',
    repoUrl: 'https://github.com/org/repo',
    role: 'maintainer',
    originalRole: 'approver',
    startDate: null,
    endDate: null,
    createdAt: new Date('2026-03-26T00:00:00Z'),
    identityPlatform: 'github',
    identityType: 'username',
    identityValue: 'janedoe',
    ...overrides,
  }
}

function contributorRow(overrides: Partial<RepoContributorRow> = {}): RepoContributorRow {
  return {
    repoId: '1',
    source: 'governance_file',
    role: 'approver',
    roleKind: 'maintainer',
    identityType: 'github-login',
    identityValue: 'janedoe',
    firstSeenAt: syncedAt,
    lastSeenAt: syncedAt,
    endedAt: null,
    ...overrides,
  }
}

function stubQx(repos: Array<{ id: string; url: string }>, upsertedPerCall = repos.length) {
  const select = vi.fn().mockResolvedValue(repos)
  const result = vi.fn().mockResolvedValue(upsertedPerCall)
  return { qx: { select, result } as unknown as QueryExecutor, select, result }
}

function stubPackagesQx(repos: Array<{ id: string; url: string }>, removed: number) {
  const select = vi
    .fn()
    .mockResolvedValueOnce([{ now: syncedAt }])
    .mockResolvedValue(repos)
  const result = vi
    .fn()
    .mockImplementation((sql: string) => Promise.resolve(sql.startsWith('DELETE') ? removed : 1))
  return { qx: { select, result } as unknown as QueryExecutor, select, result }
}

describe('dedupeContributorRows', () => {
  it('collapses rows sharing the unique key and prefers the active one', () => {
    const ended = contributorRow({ endedAt: new Date('2026-01-01T00:00:00Z') })
    const active = contributorRow()
    expect(dedupeContributorRows([ended, active])).toEqual([active])
    expect(dedupeContributorRows([active, ended])).toEqual([active])
  })

  it('keeps rows that differ in role or identity', () => {
    const rows = [
      contributorRow(),
      contributorRow({ role: 'reviewer' }),
      contributorRow({ identityValue: 'other' }),
    ]
    expect(dedupeContributorRows(rows)).toHaveLength(3)
  })
})

describe('applyGovernancePage', () => {
  it('looks repos up once per page and upserts matched rows in one statement', async () => {
    const { qx, select, result } = stubQx([{ id: '42', url: 'https://github.com/org/repo' }], 2)
    const page = [
      cdpRow({ repoUrl: 'https://github.com/Org/Repo' }),
      cdpRow({ identityValue: 'bob', endDate: new Date('2026-07-31T00:00:00Z') }),
      cdpRow({ repoUrl: 'https://github.com/unknown/repo' }),
      cdpRow({ repoUrl: 'https://gerrit.example.org/x/y' }),
    ]

    const counts = await applyGovernancePage(qx, page, syncedAt)

    expect(select).toHaveBeenCalledTimes(1)
    expect(select.mock.calls[0][1]).toEqual({
      urls: ['https://github.com/org/repo', 'https://github.com/unknown/repo'],
    })
    expect(result).toHaveBeenCalledTimes(1)
    const sql = result.mock.calls[0][0] as string
    expect(sql).toContain('INSERT INTO "repo_contributors"')
    expect(sql).toContain('ON CONFLICT (repo_id, source, identity_type, identity_value, role)')
    expect(sql).toContain(
      `repo_contributors.updated_at >= '${syncedAt.toISOString()}'::timestamptz`,
    )
    expect(sql).toContain('EXCLUDED.ended_at IS NOT NULL')
    expect(sql).toContain("'janedoe'")
    expect(sql).toContain("'bob'")
    expect(counts).toEqual({ upserted: 2, ended: 1, skippedUnknownRepo: 2 })
  })

  it('issues no write when nothing in the page matches a known repo', async () => {
    const { qx, result } = stubQx([])
    const counts = await applyGovernancePage(qx, [cdpRow()], syncedAt)
    expect(result).not.toHaveBeenCalled()
    expect(counts).toEqual({ upserted: 0, ended: 0, skippedUnknownRepo: 1 })
  })

  it('does not write the same unique key twice in one statement', async () => {
    const { qx, result } = stubQx([{ id: '42', url: 'https://github.com/org/repo' }])
    const page = [cdpRow({ identityValue: 'JaneDoe' }), cdpRow({ identityValue: 'janedoe' })]
    const counts = await applyGovernancePage(qx, page, syncedAt)
    expect(result).toHaveBeenCalledTimes(1)
    expect(counts.upserted).toBe(1)
  })
})

describe('syncGovernanceContributors', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('pages through cdp by id and sums the counts', async () => {
    const firstPage = Array.from({ length: 5000 }, (_, i) =>
      cdpRow({ id: `00000000-0000-0000-0000-${String(i + 1).padStart(12, '0')}` }),
    )
    const secondPage = [
      cdpRow({ id: 'ffffffff-ffff-ffff-ffff-ffffffffffff', identityValue: 'bob' }),
    ]
    const cdpSelect = vi
      .fn()
      .mockResolvedValueOnce(firstPage)
      .mockResolvedValueOnce(secondPage)
      .mockResolvedValueOnce([])
    const { qx: pkgsQx, result } = stubPackagesQx(
      [{ id: '42', url: 'https://github.com/org/repo' }],
      7,
    )
    vi.mocked(getCdpDb).mockResolvedValue({ select: cdpSelect } as unknown as QueryExecutor)
    vi.mocked(getPackagesDb).mockResolvedValue(pkgsQx)

    const counts = await syncGovernanceContributors()

    expect(cdpSelect).toHaveBeenCalledTimes(3)
    expect(cdpSelect.mock.calls[0][1]).toEqual({
      afterId: '00000000-0000-0000-0000-000000000000',
      limit: 5000,
    })
    expect(cdpSelect.mock.calls[1][1]).toEqual({
      afterId: '00000000-0000-0000-0000-000000005000',
      limit: 5000,
    })
    expect(cdpSelect.mock.calls[2][1]).toEqual({
      afterId: 'ffffffff-ffff-ffff-ffff-ffffffffffff',
      limit: 5000,
    })
    expect(result).toHaveBeenCalledTimes(3)
    const deleteSql = result.mock.calls[2][0] as string
    expect(deleteSql).toContain('DELETE FROM repo_contributors')
    expect(result.mock.calls[2][1]).toEqual({
      source: 'governance_file',
      runStartedAt: syncedAt,
    })
    expect(counts).toEqual({
      read: 5001,
      upserted: 2,
      ended: 0,
      removed: 7,
      skippedUnknownRepo: 0,
    })
  })

  it('refuses to remove anything when cdp returns no rows', async () => {
    const cdpSelect = vi.fn().mockResolvedValue([])
    const { qx: pkgsQx, result } = stubPackagesQx([], 0)
    vi.mocked(getCdpDb).mockResolvedValue({ select: cdpSelect } as unknown as QueryExecutor)
    vi.mocked(getPackagesDb).mockResolvedValue(pkgsQx)

    await expect(syncGovernanceContributors()).rejects.toThrow('no governance-file roles')
    expect(result).not.toHaveBeenCalled()
  })
})
