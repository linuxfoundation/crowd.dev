import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { mirrorRepo } from './mirrorRepo'

const mocks = vi.hoisted(() => ({
  getCredential: vi.fn(),
  mintInstallationToken: vi.fn(),
  resolveRepoChannel: vi.fn(),
  getNangoConnectionData: vi.fn(),
  hasSyncUnitsForChannel: vi.fn(),
  upsertSyncUnits: vi.fn(),
  alignSyncUnitChannelName: vi.fn(),
  findIntegrationRepositoryUrl: vi.fn(),
  timeout: vi.fn(),
}))

vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  timeout: mocks.timeout,
}))

vi.mock('@crowd/connectors', () => ({ getCredential: mocks.getCredential }))
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
vi.mock('@crowd/connectors/src/connectors/github/appToken', () => ({
  mintInstallationToken: mocks.mintInstallationToken,
}))
vi.mock('@crowd/connectors/src/connectors/github/discover', () => ({
  resolveRepoChannel: mocks.resolveRepoChannel,
}))
vi.mock('@crowd/nango', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/nango')>()),
  getNangoConnectionData: mocks.getNangoConnectionData,
}))
vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  findIntegrationRepositoryUrl: mocks.findIntegrationRepositoryUrl,
}))
vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  hasSyncUnitsForChannel: mocks.hasSyncUnitsForChannel,
  upsertSyncUnits: mocks.upsertSyncUnits,
  alignSyncUnitChannelName: mocks.alignSyncUnitChannelName,
}))

const qx = {} as QueryExecutor
const candidate = { integrationId: 'i-1', connectionId: 'c-1', owner: 'acme', repoName: 'widget' }
const since = '2026-10-18T00:00:00.000Z'
const httpError = (status: number) =>
  new axios.AxiosError(`status ${status}`, String(status), undefined, undefined, {
    status,
  } as never)

describe('mirrorRepo', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.getCredential.mockResolvedValue({ appId: 'app' })
    mocks.getNangoConnectionData.mockResolvedValue({
      connection_config: { installation_id: 'inst-1' },
    })
    mocks.mintInstallationToken.mockResolvedValue({ token: 'tok' })
    mocks.resolveRepoChannel.mockResolvedValue({
      channelId: 'R_1',
      channelName: 'https://github.com/Acme/Widget',
    })
    mocks.hasSyncUnitsForChannel.mockResolvedValue(false)
    mocks.findIntegrationRepositoryUrl.mockResolvedValue('https://github.com/acme/WIDGET')
    mocks.upsertSyncUnits.mockImplementation(async (_qx, units: unknown[]) => units.length)
  })

  it('mirrors one incremental unit per github sync', async () => {
    await expect(mirrorRepo(qx, candidate, since)).resolves.toEqual({
      outcome: 'mirrored',
      unitsInserted: 8,
    })

    expect(mocks.mintInstallationToken).toHaveBeenCalledWith({ appId: 'app' }, 'inst-1')
    expect(mocks.resolveRepoChannel).toHaveBeenCalledWith('tok', 'acme', 'widget')
    expect(mocks.hasSyncUnitsForChannel).toHaveBeenCalledWith(qx, 'i-1', 'R_1')
    const units = mocks.upsertSyncUnits.mock.calls[0][1]
    expect(units).toHaveLength(8)
    expect(new Set(units.map((u: { syncName: string }) => u.syncName)).size).toBe(8)
    expect(units[0]).toEqual({
      integrationId: 'i-1',
      platform: 'github',
      channelId: 'R_1',
      channelName: 'https://github.com/acme/WIDGET',
      syncName: 'discussions',
      watermark: { phase: 'incremental', since, cursor: null },
    })
    expect(mocks.findIntegrationRepositoryUrl).toHaveBeenCalledWith(
      qx,
      'i-1',
      'https://github.com/Acme/Widget',
    )

    mocks.upsertSyncUnits.mockClear()
    mocks.findIntegrationRepositoryUrl.mockResolvedValue('https://github.com/Acme/Widget')
    await mirrorRepo(qx, candidate, since)
    expect(mocks.upsertSyncUnits.mock.calls[0][1][0].channelName).toBe(
      'https://github.com/Acme/Widget',
    )

    mocks.upsertSyncUnits.mockClear()
    mocks.findIntegrationRepositoryUrl.mockResolvedValue(null)
    await mirrorRepo(qx, candidate, since)
    expect(mocks.upsertSyncUnits.mock.calls[0][1][0].channelName).toBe(
      'https://github.com/acme/widget',
    )
  })

  it('skips a repo whose channel resolves to another name', async () => {
    mocks.resolveRepoChannel.mockResolvedValue({
      channelId: 'R_1',
      channelName: 'https://github.com/acme/widget-next',
    })

    await expect(mirrorRepo(qx, candidate, since)).resolves.toEqual({
      outcome: 'skipped',
      reason: 'repo_renamed',
    })

    mocks.getCredential.mockRejectedValue(new Error('connection terminated'))
    await expect(mirrorRepo(qx, candidate, since)).rejects.toThrow('connection terminated')
    expect(mocks.hasSyncUnitsForChannel).not.toHaveBeenCalled()
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
  })

  it('skips a channel already mirrored and a repo github no longer finds', async () => {
    mocks.hasSyncUnitsForChannel.mockResolvedValue(true)
    await expect(mirrorRepo(qx, candidate, since)).resolves.toEqual({
      outcome: 'skipped',
      reason: 'already_mirrored',
      message: 'units exist for https://github.com/Acme/Widget',
    })
    expect(mocks.alignSyncUnitChannelName).toHaveBeenCalledWith(
      qx,
      'i-1',
      'R_1',
      'https://github.com/acme/WIDGET',
    )

    mocks.resolveRepoChannel.mockRejectedValue(
      new axios.AxiosError('Not Found', '404', undefined, undefined, { status: 404 } as never),
    )
    await expect(mirrorRepo(qx, candidate, since)).resolves.toEqual({
      outcome: 'skipped',
      reason: 'repo_not_found',
      message: 'Not Found',
    })
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
  })

  it('retries nango server errors and hung requests with backoff and skips once they persist', async () => {
    mocks.getNangoConnectionData
      .mockRejectedValueOnce(httpError(503))
      .mockRejectedValueOnce(httpError(504))
    await expect(mirrorRepo(qx, candidate, since)).resolves.toMatchObject({ outcome: 'mirrored' })
    expect(mocks.timeout.mock.calls).toEqual([[5_000], [15_000]])

    vi.clearAllMocks()
    mocks.getNangoConnectionData.mockRejectedValue(httpError(503))
    await expect(mirrorRepo(qx, candidate, since)).resolves.toEqual({
      outcome: 'skipped',
      reason: 'nango_error',
      message: 'status 503',
    })
    expect(mocks.getNangoConnectionData).toHaveBeenCalledTimes(4)

    vi.clearAllMocks()
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    mocks.getNangoConnectionData
      .mockReturnValueOnce(new Promise(() => {}))
      .mockResolvedValueOnce({ connection_config: { installation_id: 'inst-1' } })
    const hung = mirrorRepo(qx, candidate, since)
    await vi.advanceTimersByTimeAsync(30_000)
    await expect(hung).resolves.toMatchObject({ outcome: 'mirrored' })
    expect(mocks.timeout.mock.calls).toEqual([[5_000]])
    expect(vi.getTimerCount()).toBe(0)
    vi.useRealTimers()

    vi.clearAllMocks()
    mocks.getNangoConnectionData.mockRejectedValue(httpError(404))
    await expect(mirrorRepo(qx, candidate, since)).resolves.toMatchObject({ reason: 'nango_error' })
    expect(mocks.getNangoConnectionData).toHaveBeenCalledTimes(1)
    expect(mocks.timeout).not.toHaveBeenCalled()
  })
})
