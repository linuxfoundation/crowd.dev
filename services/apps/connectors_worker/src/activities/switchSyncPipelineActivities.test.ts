import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  evaluateForwardGate,
  pauseNangoRepoSync,
  readNangoHandoverPoint,
  resolveSwitchTarget,
  setUnitEmitEnabled,
  takeOverUnit,
} from './switchSyncPipelineActivities'

const mocks = vi.hoisted(() => ({
  findSyncUnitByChannelName: vi.fn(),
  getSyncDiffCountsForDays: vi.fn(),
  setSyncUnitEmitEnabled: vi.fn(),
  takeOverSyncUnit: vi.fn(),
  findActiveNangoMappingForRepo: vi.fn(),
  getNangoModelLastCheckedAt: vi.fn(),
  initNangoCloudClient: vi.fn(),
  pauseNangoSync: vi.fn(),
  startNangoSync: vi.fn(),
  getNangoConnectionStatus: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    log: mocks.log,
  },
}))
vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({ dbStoreQx: vi.fn(() => 'qx') }))
vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  findSyncUnitByChannelName: mocks.findSyncUnitByChannelName,
  getSyncDiffCountsForDays: mocks.getSyncDiffCountsForDays,
  setSyncUnitEmitEnabled: mocks.setSyncUnitEmitEnabled,
  takeOverSyncUnit: mocks.takeOverSyncUnit,
}))
vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  findActiveNangoMappingForRepo: mocks.findActiveNangoMappingForRepo,
  getNangoModelLastCheckedAt: mocks.getNangoModelLastCheckedAt,
}))
vi.mock('@crowd/nango', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/nango')>()),
  initNangoCloudClient: mocks.initNangoCloudClient,
  pauseNangoSync: mocks.pauseNangoSync,
  startNangoSync: mocks.startNangoSync,
  getNangoConnectionStatus: mocks.getNangoConnectionStatus,
}))

const activeUnit = {
  id: 'u-1',
  status: 'active',
  emitEnabled: false,
  watermark: { phase: 'incremental' },
}

describe('switch activities', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ now: new Date('2026-10-19T10:00:00Z') })
  })
  afterEach(() => vi.useRealTimers())

  it('resolves the repo through its nango mapping and builds the channel name from it', async () => {
    mocks.findActiveNangoMappingForRepo.mockResolvedValue({
      integrationId: 'int-1',
      connectionId: 'conn-1',
      owner: 'Kubernetes',
      repoName: 'kubernetes',
    })

    await expect(
      resolveSwitchTarget({ owner: 'kubernetes', repo: 'KUBERNETES', syncNames: ['issues'] }),
    ).resolves.toEqual({
      target: {
        integrationId: 'int-1',
        connectionId: 'conn-1',
        channelName: 'https://github.com/Kubernetes/kubernetes',
      },
      syncs: [{ syncName: 'issues', nangoSync: 'issues' }],
    })
  })

  it('gates on the unit, nango consumption, then the three most recent diffed days', async () => {
    mocks.findSyncUnitByChannelName.mockResolvedValue(activeUnit)
    mocks.getNangoModelLastCheckedAt.mockResolvedValue('2026-10-19T09:30:00Z')
    mocks.getNangoModelLastCheckedAt.mockResolvedValue('2026-10-19T09:30:00Z')
    mocks.getSyncDiffCountsForDays.mockResolvedValue(
      ['2026-10-17', '2026-10-16', '2026-10-15'].map((day) => ({
        day,
        missingInShadowCount: 0,
        unsupportedSyncCount: 0,
      })),
    )

    await expect(
      evaluateForwardGate({
        integrationId: 'int-1',
        connectionId: 'conn-1',
        channelName: 'https://github.com/o/r',
        syncName: 'issues',
      }),
    ).resolves.toEqual({ ok: true, unitId: 'u-1' })
    expect(mocks.findSyncUnitByChannelName).toHaveBeenCalledWith(
      'qx',
      'int-1',
      'github',
      'https://github.com/o/r',
      'issues',
    )
    expect(mocks.getNangoModelLastCheckedAt).toHaveBeenCalledWith(
      'qx',
      'int-1',
      'conn-1',
      'GithubIssue',
    )
    expect(mocks.getSyncDiffCountsForDays).toHaveBeenCalledWith('qx', 'u-1', [
      '2026-10-17',
      '2026-10-16',
      '2026-10-15',
    ])

    mocks.findSyncUnitByChannelName.mockResolvedValue({ ...activeUnit, status: 'dead_letter' })
    await expect(
      evaluateForwardGate({
        integrationId: 'int-1',
        connectionId: 'conn-1',
        channelName: 'https://github.com/o/r',
        syncName: 'issues',
      }),
    ).resolves.toMatchObject({ ok: false, outcome: 'refused' })
    expect(mocks.getSyncDiffCountsForDays).toHaveBeenCalledTimes(1)
  })

  it('pauses the one nango sync on an initialised client and logs a no-op flag flip', async () => {
    await pauseNangoRepoSync({ connectionId: 'conn-1', nangoSync: 'pull-request-reviews' })
    expect(mocks.initNangoCloudClient).toHaveBeenCalled()
    expect(mocks.pauseNangoSync).toHaveBeenCalledWith('github', 'conn-1', ['pull-request-reviews'])

    mocks.setSyncUnitEmitEnabled.mockResolvedValue(0)
    await setUnitEmitEnabled({ unitId: 'u-1', emitEnabled: true })
    expect(mocks.setSyncUnitEmitEnabled).toHaveBeenCalledWith('qx', 'u-1', true)
    expect(mocks.log.info).toHaveBeenCalledWith(
      expect.objectContaining({ alreadyInState: true, unitId: 'u-1' }),
      'sync unit emit flag set',
    )
  })

  it('hands over from the earlier of the nango run and cdp consumption, and takes the unit over an hour before it', async () => {
    mocks.getNangoConnectionStatus.mockResolvedValue([
      { name: 'issues', status: 'PAUSED', finishedAt: '2026-10-19T09:00:00.000Z' },
      { name: 'forks', status: 'SUCCESS' },
    ])
    mocks.getNangoModelLastCheckedAt.mockResolvedValue('2026-10-19T07:00:00Z')
    const point = { integrationId: 'int-1', connectionId: 'conn-1' }
    await expect(
      readNangoHandoverPoint({ ...point, syncName: 'issues', nangoSync: 'issues' }),
    ).resolves.toEqual({ handoverAt: '2026-10-19T07:00:00Z' })
    expect(mocks.getNangoConnectionStatus).toHaveBeenCalledWith('github', 'conn-1')
    expect(mocks.getNangoModelLastCheckedAt).toHaveBeenCalledWith(
      'qx',
      'int-1',
      'conn-1',
      'GithubIssue',
    )
    await expect(
      readNangoHandoverPoint({ ...point, syncName: 'forks', nangoSync: 'forks' }),
    ).resolves.toEqual({ handoverAt: null })

    mocks.takeOverSyncUnit.mockResolvedValue(1)
    await expect(
      takeOverUnit({ unitId: 'u-1', handoverAt: '2026-10-19T07:00:00Z' }),
    ).resolves.toEqual({ since: '2026-10-19T06:00:00.000Z' })
    expect(mocks.takeOverSyncUnit).toHaveBeenCalledWith('qx', 'u-1', '2026-10-19T06:00:00.000Z')
    await expect(takeOverUnit({ unitId: 'u-2', handoverAt: null })).resolves.toEqual({
      since: null,
    })
    expect(mocks.takeOverSyncUnit).toHaveBeenLastCalledWith('qx', 'u-2', null)
  })
})
