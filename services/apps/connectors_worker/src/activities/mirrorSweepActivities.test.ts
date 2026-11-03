import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { IMirrorCandidate } from '../mirrorSweep'
import { mirrorBatch } from './mirrorSweepActivities'

const mocks = vi.hoisted(() => ({
  listUnmirroredNangoRepos: vi.fn(),
  countShadowRepos: vi.fn(),
  executeSql: vi.fn(),
  mirrorRepo: vi.fn(),
  applyGithubRenames: vi.fn(),
  installationTokenSource: vi.fn(),
  initNangoCloudClient: vi.fn(),
  heartbeat: vi.fn(),
  cancellation: new AbortController(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/activity', () => ({
  Context: {
    current: () => ({
      heartbeat: mocks.heartbeat,
      cancellationSignal: mocks.cancellation.signal,
    }),
  },
}))
vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    log: mocks.log,
  },
}))
vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({ dbStoreQx: vi.fn(() => 'qx') }))
vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  countShadowRepos: mocks.countShadowRepos,
}))
vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  listUnmirroredNangoRepos: mocks.listUnmirroredNangoRepos,
}))
vi.mock('@crowd/database', () => ({
  TinybirdClient: class {
    executeSql = mocks.executeSql
  },
}))
vi.mock('../mirrorRepo', () => ({ mirrorRepo: mocks.mirrorRepo }))
vi.mock('../githubRenames', () => ({ applyGithubRenames: mocks.applyGithubRenames }))
vi.mock('../githubInstallationToken', () => ({
  installationTokenSource: mocks.installationTokenSource,
}))
vi.mock('@crowd/nango', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/nango')>()),
  initNangoCloudClient: mocks.initNangoCloudClient,
}))

const repo = (repoName: string) => ({
  integrationId: 'int-1',
  connectionId: 'conn-1',
  owner: 'acme',
  repoName,
  enabled: true,
  archived: false,
  excluded: false,
})

const candidate = (repoName: string): IMirrorCandidate => ({
  integrationId: 'int-1',
  connectionId: 'conn-1',
  owner: 'acme',
  repoName,
})

describe('mirror sweep activities', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.cancellation = new AbortController()
    vi.useFakeTimers({
      now: new Date('2026-10-20T10:00:00Z'),
      toFake: ['Date', 'setInterval', 'clearInterval'],
    })
  })
  afterEach(() => vi.useRealTimers())

  it('mirrors the most active candidates until the limit is mirrored, refilling skips, five at a time, logging every skip', async () => {
    mocks.listUnmirroredNangoRepos.mockResolvedValue(
      ['r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 'r8'].map(repo),
    )
    mocks.executeSql.mockResolvedValue({
      data: [
        { channelKey: 'https://github.com/acme/r8', activities: 50 },
        { channelKey: 'https://github.com/acme/r2', activities: 10 },
      ],
    })
    let inFlight = 0
    let peak = 0
    mocks.mirrorRepo.mockImplementation(async (_qx: unknown, c: IMirrorCandidate) => {
      inFlight++
      peak = Math.max(peak, inFlight)
      if (c.repoName === 'r3') {
        vi.advanceTimersByTime(30_000)
      }
      await Promise.resolve()
      inFlight--
      return ['r2', 'r5'].includes(c.repoName)
        ? { outcome: 'skipped', reason: 'repo_not_found', message: '404' }
        : { outcome: 'mirrored', unitsInserted: 8 }
    })

    await expect(mirrorBatch(5)).resolves.toEqual({
      candidates: 8,
      attempted: 7,
      mirrored: 5,
      skipped: 2,
      unitsInserted: 40,
      renamesApplied: 0,
      renamesRefused: 0,
      renamesFailed: 0,
      failed: false,
    })

    expect(peak).toBe(5)
    expect(mocks.heartbeat).toHaveBeenCalledTimes(1)
    expect(vi.getTimerCount()).toBe(0)
    expect(mocks.initNangoCloudClient).toHaveBeenCalledTimes(1)
    expect(mocks.mirrorRepo.mock.calls.map(([, c]) => c.repoName)).toEqual([
      'r8',
      'r2',
      'r1',
      'r3',
      'r4',
      'r5',
      'r6',
    ])
    expect(mocks.mirrorRepo).toHaveBeenCalledWith('qx', candidate('r8'), '2026-10-18T00:00:00.000Z')
    expect(mocks.log.warn).toHaveBeenCalledTimes(2)
    expect(mocks.log.warn).toHaveBeenCalledWith(
      {
        event: 'mirror_repo_skipped',
        integrationId: 'int-1',
        owner: 'acme',
        repoName: 'r2',
        reason: 'repo_not_found',
        message: '404',
      },
      'repo skipped by mirror sweep',
    )
  })

  it('stops at the first error or cancellation with the progress so far and lets selection errors fail the activity', async () => {
    mocks.listUnmirroredNangoRepos.mockResolvedValue(['r1', 'r2', 'r3', 'r4', 'r5', 'r6'].map(repo))
    mocks.executeSql.mockResolvedValue({ data: [] })
    mocks.mirrorRepo.mockImplementation(async (_qx: unknown, c: IMirrorCandidate) => {
      if (c.repoName === 'r6') {
        throw new Error('connection terminated')
      }
      return { outcome: 'mirrored', unitsInserted: 8 }
    })

    await expect(mirrorBatch(10)).resolves.toEqual({
      candidates: 6,
      attempted: 5,
      mirrored: 5,
      skipped: 0,
      unitsInserted: 40,
      renamesApplied: 0,
      renamesRefused: 0,
      renamesFailed: 0,
      failed: true,
    })
    expect(mocks.log.error).toHaveBeenCalledTimes(1)

    vi.clearAllMocks()
    mocks.executeSql.mockRejectedValue(new Error('tinybird down'))

    await expect(mirrorBatch(2)).rejects.toThrow('tinybird down')
    expect(mocks.mirrorRepo).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)

    mocks.executeSql.mockResolvedValue({ data: [] })
    mocks.cancellation.abort()
    await expect(mirrorBatch(2)).resolves.toMatchObject({ attempted: 0, failed: true })
    expect(mocks.mirrorRepo).not.toHaveBeenCalled()
  })

  it('mirrorBatch applies renames once per integration after the batch', async () => {
    const repos = [
      { ...repo('a'), integrationId: 'int-1' },
      { ...repo('b'), integrationId: 'int-1' },
      { ...repo('c'), integrationId: 'int-2' },
      { ...repo('d'), integrationId: 'int-1' },
    ]
    mocks.listUnmirroredNangoRepos.mockResolvedValue(repos)
    mocks.executeSql.mockResolvedValue({ data: [] })
    mocks.installationTokenSource.mockImplementation((_qx, id: string) => `token-${id}`)
    mocks.applyGithubRenames.mockImplementation(async (_qx, id: string) =>
      id === 'int-1'
        ? { applied: 1, refused: 1, failed: 0 }
        : { applied: 0, refused: 0, failed: 1 },
    )
    mocks.mirrorRepo.mockImplementation(async (_qx: unknown, c: IMirrorCandidate) =>
      c.repoName === 'd'
        ? { outcome: 'mirrored', unitsInserted: 8 }
        : { outcome: 'renamed', from: `old-${c.repoName}`, to: `new-${c.repoName}` },
    )

    await expect(mirrorBatch(5)).resolves.toEqual({
      candidates: 4,
      attempted: 4,
      mirrored: 1,
      skipped: 0,
      unitsInserted: 8,
      renamesApplied: 1,
      renamesRefused: 1,
      renamesFailed: 1,
      failed: false,
    })

    expect(mocks.applyGithubRenames).toHaveBeenCalledTimes(2)
    expect(mocks.applyGithubRenames).toHaveBeenCalledWith(
      'qx',
      'int-1',
      [
        { from: 'old-a', to: 'new-a' },
        { from: 'old-b', to: 'new-b' },
      ],
      'token-int-1',
    )
    expect(mocks.applyGithubRenames).toHaveBeenCalledWith(
      'qx',
      'int-2',
      [{ from: 'old-c', to: 'new-c' }],
      'token-int-2',
    )
    expect(mocks.log.warn).not.toHaveBeenCalled()
  })
})
