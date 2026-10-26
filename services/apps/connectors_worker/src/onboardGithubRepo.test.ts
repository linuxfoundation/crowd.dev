import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { LIFECYCLE_STALE_LOCK_MS } from './githubLifecycle'
import { onboardGithubRepo } from './onboardGithubRepo'

const mocks = vi.hoisted(() => ({
  resolveRepo: vi.fn(),
  hasActiveSyncUnitsForChannelElsewhere: vi.fn(),
  hasSyncUnitsForChannel: vi.fn(),
  lockSyncUnitChannel: vi.fn(),
  alignSyncUnitChannelName: vi.fn(),
  upsertSyncUnits: vi.fn(),
}))

vi.mock('@crowd/connectors/src/connectors/github', () => ({
  githubConnector: {
    platform: 'github',
    syncs: [
      'discussions',
      'forks',
      'issues',
      'issue-comments',
      'pull-requests',
      'pull-request-comments',
      'pull-request-review-comments',
      'pull-request-commits',
    ].map((name) => ({ name })),
  },
}))
vi.mock('@crowd/connectors/src/connectors/github/discover', () => ({
  resolveRepo: mocks.resolveRepo,
}))
vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  hasActiveSyncUnitsForChannelElsewhere: mocks.hasActiveSyncUnitsForChannelElsewhere,
  hasSyncUnitsForChannel: mocks.hasSyncUnitsForChannel,
  lockSyncUnitChannel: mocks.lockSyncUnitChannel,
  alignSyncUnitChannelName: mocks.alignSyncUnitChannelName,
  upsertSyncUnits: mocks.upsertSyncUnits,
}))

const txQx = { name: 'tx' } as unknown as QueryExecutor
const tx = vi.fn(async (fn: (inner: QueryExecutor) => Promise<unknown>) => fn(txQx))
const qx = { tx } as unknown as QueryExecutor
const key = 'https://github.com/acme/widget'

describe('onboardGithubRepo', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.resolveRepo.mockResolvedValue({ channelId: 'R_1', channelName: key, forkedFrom: null })
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(false)
    mocks.hasSyncUnitsForChannel.mockResolvedValue(false)
    mocks.upsertSyncUnits.mockImplementation(async (_qx, units: unknown[]) => units.length)
  })

  it('creates one emitting unit per github sync with no watermark', async () => {
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'created',
      unitsInserted: 8,
    })

    expect(mocks.resolveRepo).toHaveBeenCalledWith('tok', 'acme', 'widget')
    expect(mocks.lockSyncUnitChannel).toHaveBeenCalledWith(txQx, 'R_1')
    expect(mocks.hasActiveSyncUnitsForChannelElsewhere).toHaveBeenCalledWith(
      txQx,
      'i-1',
      'R_1',
      expect.any(Date),
    )
    const staleLockBefore: Date = mocks.hasActiveSyncUnitsForChannelElsewhere.mock.calls[0][3]
    expect(Date.now() - staleLockBefore.getTime()).toBeGreaterThanOrEqual(LIFECYCLE_STALE_LOCK_MS)
    expect(mocks.hasSyncUnitsForChannel).toHaveBeenCalledWith(txQx, 'i-1', 'R_1')
    expect(mocks.upsertSyncUnits.mock.calls[0][0]).toBe(txQx)
    const callOrder = [
      mocks.lockSyncUnitChannel,
      mocks.hasActiveSyncUnitsForChannelElsewhere,
      mocks.hasSyncUnitsForChannel,
      mocks.upsertSyncUnits,
    ].map((fn) => fn.mock.invocationCallOrder[0])
    expect(callOrder).toEqual([...callOrder].sort((a, b) => a - b))
    const units = mocks.upsertSyncUnits.mock.calls[0][1]
    expect(units).toHaveLength(8)
    expect(units[0]).toEqual({
      integrationId: 'i-1',
      platform: 'github',
      channelId: 'R_1',
      channelName: key,
      syncName: 'discussions',
      emitEnabled: true,
    })
    expect(units.every((u: object) => !('watermark' in u))).toBe(true)
    expect(units.every((u: object) => !('initialDelaySeconds' in u))).toBe(true)

    await onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key, initialDelaySeconds: 42 })
    expect(
      mocks.upsertSyncUnits.mock.calls[1][1].every(
        (u: { initialDelaySeconds: number }) => u.initialDelaySeconds === 42,
      ),
    ).toBe(true)

    mocks.resolveRepo.mockClear()
    await expect(
      onboardGithubRepo(qx, 'tok', 'i-1', key, {
        storedUrl: key,
        initialDelaySeconds: 4,
        resolved: { channelId: 'R_9', channelName: key },
      }),
    ).resolves.toEqual({ outcome: 'created', unitsInserted: 8 })
    expect(mocks.resolveRepo).not.toHaveBeenCalled()
    expect(mocks.upsertSyncUnits.mock.calls[2][1][0]).toMatchObject({ channelId: 'R_9' })
    await expect(
      onboardGithubRepo(qx, 'tok', 'i-1', key, {
        storedUrl: key,
        initialDelaySeconds: 4,
        resolved: { channelId: 'R_9', channelName: 'https://github.com/acme/widget-next' },
      }),
    ).resolves.toEqual({ outcome: 'skipped', reason: 'repo_renamed' })
    expect(mocks.resolveRepo).not.toHaveBeenCalled()

    mocks.upsertSyncUnits.mockClear()
    await expect(
      onboardGithubRepo(qx, 'tok', 'i-1', 'https://github.com/org/tag-security', {
        storedUrl: 'https://github.com/org/TAG-security',
        resolved: { channelId: 'R_T', channelName: 'https://github.com/org/tag-security' },
      }),
    ).resolves.toEqual({ outcome: 'created', unitsInserted: 8 })
    expect(
      mocks.upsertSyncUnits.mock.calls[0][1].map(
        ({ channelName }: { channelName: string }) => channelName,
      ),
    ).toEqual(Array(8).fill('https://github.com/org/TAG-security'))

    mocks.upsertSyncUnits.mockClear()
    await onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })
    expect(mocks.upsertSyncUnits.mock.calls[0][1][0].channelName).toBe(key)
    expect(mocks.alignSyncUnitChannelName).not.toHaveBeenCalled()
  })

  it('skips renamed repos, repos that already have units, 404s and other github errors with the matching reason', async () => {
    mocks.resolveRepo.mockResolvedValue({
      channelId: 'R_1',
      channelName: 'https://github.com/acme/widget-next',
      forkedFrom: null,
    })
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'repo_renamed',
    })

    mocks.resolveRepo.mockResolvedValue({ channelId: 'R_1', channelName: key, forkedFrom: null })
    mocks.hasSyncUnitsForChannel.mockResolvedValue(true)
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'already_present',
      message: `units exist for ${key}`,
    })
    expect(mocks.alignSyncUnitChannelName).toHaveBeenCalledWith(txQx, 'i-1', 'R_1', key)
    mocks.alignSyncUnitChannelName.mockClear()

    mocks.hasSyncUnitsForChannel.mockResolvedValue(false)
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(true)
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'owned_by_other_integration',
      message: `units exist for ${key} under another integration`,
    })
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(false)

    mocks.upsertSyncUnits.mockResolvedValueOnce(3)
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'already_present',
      message: 'inserted 3 of 8',
    })
    expect(mocks.alignSyncUnitChannelName).toHaveBeenCalledWith(txQx, 'i-1', 'R_1', key)
    mocks.upsertSyncUnits.mockClear()

    mocks.resolveRepo.mockRejectedValue(
      new axios.AxiosError('Not Found', '404', undefined, undefined, { status: 404 } as never),
    )
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'repo_not_found',
      message: 'Not Found',
    })

    vi.useFakeTimers({ now: new Date('2026-10-22T10:00:00Z'), toFake: ['Date'] })
    mocks.resolveRepo.mockRejectedValue(
      new axios.AxiosError('Too Many Requests', '429', undefined, undefined, {
        status: 429,
        headers: { 'retry-after': '120' },
      } as never),
    )
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'rate_limited',
      message: 'Too Many Requests',
      resetAt: '2026-10-22T10:02:00.000Z',
    })
    vi.useRealTimers()

    mocks.resolveRepo.mockRejectedValue(new Error('boom'))
    await expect(onboardGithubRepo(qx, 'tok', 'i-1', key, { storedUrl: key })).resolves.toEqual({
      outcome: 'skipped',
      reason: 'github_error',
      message: 'boom',
    })
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
  })
})
