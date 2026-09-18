import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { getNangoCloudRecords } from '@crowd/nango'

import { listShadowDiffChannels, runShadowDiffForChannel } from './shadowDiffActivities'

const mocks = vi.hoisted(() => ({
  listShadowDiffUnits: vi.fn(),
  getNangoMappingForRepo: vi.fn(),
  getShadowRecordsInWindow: vi.fn(),
  getUnitIdsWithSummary: vi.fn(),
  pruneMatchingShadowRecords: vi.fn(),
  upsertSyncDiffSummary: vi.fn(),
  initNangoCloudClient: vi.fn(),
  getNangoCloudRecords: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  },
}))

const qx = { tx: (fn: (tx: unknown) => Promise<unknown>) => fn(qx) }

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  dbStoreQx: vi.fn(() => qx),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  listShadowDiffUnits: mocks.listShadowDiffUnits,
  getShadowRecordsInWindow: mocks.getShadowRecordsInWindow,
  getUnitIdsWithSummary: mocks.getUnitIdsWithSummary,
  pruneMatchingShadowRecords: mocks.pruneMatchingShadowRecords,
  upsertSyncDiffSummary: mocks.upsertSyncDiffSummary,
}))

vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  getNangoMappingForRepo: mocks.getNangoMappingForRepo,
}))

vi.mock('@crowd/nango', () => ({
  NangoIntegration: { GITHUB: 'github' },
  NangoMetadataLastAction: { ADDED: 'ADDED', UPDATED: 'UPDATED', DELETED: 'DELETED' },
  initNangoCloudClient: mocks.initNangoCloudClient,
  getNangoCloudRecords: mocks.getNangoCloudRecords,
}))

const UNIT: {
  id: string
  integrationId: string
  channelName: string
  syncName: string
} = {
  id: 'unit-1',
  integrationId: 'integration-1',
  channelName: 'https://github.com/kubernetes/kubernetes',
  syncName: 'issues',
}

describe('listShadowDiffChannels', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('groups shadow-diff units by channelName', async () => {
    mocks.listShadowDiffUnits.mockResolvedValue([
      UNIT,
      { ...UNIT, id: 'unit-2', syncName: 'issue-comments' },
      {
        id: 'unit-3',
        integrationId: 'integration-1',
        channelName: 'https://github.com/torvalds/linux',
        syncName: 'issues',
      },
    ])

    const channels = await listShadowDiffChannels()

    expect(channels).toEqual([
      {
        channelName: 'https://github.com/kubernetes/kubernetes',
        integrationId: 'integration-1',
        units: [UNIT, { ...UNIT, id: 'unit-2', syncName: 'issue-comments' }],
      },
      {
        channelName: 'https://github.com/torvalds/linux',
        integrationId: 'integration-1',
        units: [
          {
            id: 'unit-3',
            integrationId: 'integration-1',
            channelName: 'https://github.com/torvalds/linux',
            syncName: 'issues',
          },
        ],
      },
    ])
  })

  it('keeps units with the same channelName but different integrationId as separate channels', async () => {
    mocks.listShadowDiffUnits.mockResolvedValue([
      UNIT,
      { ...UNIT, id: 'unit-2', integrationId: 'integration-2' },
    ])

    const channels = await listShadowDiffChannels()

    expect(channels).toEqual([
      {
        channelName: UNIT.channelName,
        integrationId: 'integration-1',
        units: [UNIT],
      },
      {
        channelName: UNIT.channelName,
        integrationId: 'integration-2',
        units: [{ ...UNIT, id: 'unit-2', integrationId: 'integration-2' }],
      },
    ])
  })
})

describe('runShadowDiffForChannel', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-15T12:00:00.000Z'))
    mocks.getUnitIdsWithSummary.mockResolvedValue(new Set())
    mocks.upsertSyncDiffSummary.mockResolvedValue(undefined)
    mocks.pruneMatchingShadowRecords.mockResolvedValue(0)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('returns mapping_missing when no nango_mapping row exists for the repo', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue(null)

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result).toEqual({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      status: 'mapping_missing',
    })
    expect(mocks.getShadowRecordsInWindow).not.toHaveBeenCalled()
    expect(mocks.upsertSyncDiffSummary).not.toHaveBeenCalled()
  })

  it('persists a diff summary and prunes matching shadow records for the window', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue([
      {
        type: 'issues-comment',
        sourceId: 'issue-1',
        occurredAt: '2026-09-13T12:00:00.000Z',
        data: { type: 'issues-comment', sourceId: 'issue-1', body: 'shadow body' },
      },
    ])
    mocks.getNangoCloudRecords.mockResolvedValue({
      records: [
        {
          timestamp: new Date('2026-09-13T12:00:00.000Z').getTime(),
          activity: { type: 'issues-comment', sourceId: 'issue-1', body: 'nango body' },
          metadata: { lastModifiedAt: '2026-09-13T12:00:00.000Z' },
        },
      ],
      nextCursor: undefined,
    })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result).toEqual({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      status: 'ok',
    })
    expect(mocks.initNangoCloudClient).toHaveBeenCalled()
    expect(getNangoCloudRecords).toHaveBeenCalledWith(
      'github',
      'conn-1',
      'GithubIssue',
      undefined,
      undefined,
      '2026-09-13T00:00:00.000Z',
    )
    expect(mocks.upsertSyncDiffSummary).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({
        unitId: UNIT.id,
        day: '2026-09-13',
        integrationId: UNIT.integrationId,
        channelName: UNIT.channelName,
        missingInNangoCount: 0,
        missingInShadowCount: 0,
        fieldMismatchCount: 1,
        unsupportedSyncCount: 0,
      }),
    )
    expect(mocks.pruneMatchingShadowRecords).toHaveBeenCalledWith(
      qx,
      UNIT.id,
      new Date('2026-09-13T00:00:00.000Z'),
      new Date('2026-09-14T00:00:00.000Z'),
      [],
    )
  })

  it('prunes only shadow records that were confirmed clean in the snapshot, not concurrently inserted ones', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue([
      {
        type: 'issues-comment',
        sourceId: 'clean-1',
        occurredAt: '2026-09-13T12:00:00.000Z',
        data: { type: 'issues-comment', sourceId: 'clean-1', body: 'same body' },
      },
      {
        type: 'issues-comment',
        sourceId: 'mismatched-1',
        occurredAt: '2026-09-13T12:00:00.000Z',
        data: { type: 'issues-comment', sourceId: 'mismatched-1', body: 'shadow body' },
      },
    ])
    mocks.getNangoCloudRecords.mockResolvedValue({
      records: [
        {
          timestamp: new Date('2026-09-13T12:00:00.000Z').getTime(),
          activity: { type: 'issues-comment', sourceId: 'clean-1', body: 'same body' },
          metadata: { lastModifiedAt: '2026-09-13T12:00:00.000Z' },
        },
        {
          timestamp: new Date('2026-09-13T12:00:00.000Z').getTime(),
          activity: { type: 'issues-comment', sourceId: 'mismatched-1', body: 'nango body' },
          metadata: { lastModifiedAt: '2026-09-13T12:00:00.000Z' },
        },
      ],
      nextCursor: undefined,
    })

    await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(mocks.pruneMatchingShadowRecords).toHaveBeenCalledWith(
      qx,
      UNIT.id,
      new Date('2026-09-13T00:00:00.000Z'),
      new Date('2026-09-14T00:00:00.000Z'),
      [{ type: 'issues-comment', sourceId: 'clean-1' }],
    )
  })

  it('resolves the diff window from an explicit targetDay instead of the previous UTC day', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue([])
    mocks.getNangoCloudRecords.mockResolvedValue({ records: [], nextCursor: undefined })

    await runShadowDiffForChannel(
      {
        channelName: UNIT.channelName,
        integrationId: UNIT.integrationId,
        units: [UNIT],
      },
      '2026-01-05',
    )

    expect(getNangoCloudRecords).toHaveBeenCalledWith(
      'github',
      'conn-1',
      'GithubIssue',
      undefined,
      undefined,
      '2026-01-05T00:00:00.000Z',
    )
    expect(mocks.upsertSyncDiffSummary).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ unitId: UNIT.id, day: '2026-01-05' }),
    )
    expect(mocks.pruneMatchingShadowRecords).toHaveBeenCalledWith(
      qx,
      UNIT.id,
      new Date('2026-01-05T00:00:00.000Z'),
      new Date('2026-01-06T00:00:00.000Z'),
      [],
    )
  })

  it('excludes soft-deleted nango records from the diff to avoid false missing_in_shadow reports', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue([])
    mocks.getNangoCloudRecords.mockResolvedValue({
      records: [
        {
          timestamp: new Date('2026-09-13T12:00:00.000Z').getTime(),
          activity: { type: 'issues-comment', sourceId: 'issue-1', body: 'nango body' },
          metadata: { lastModifiedAt: '2026-09-13T12:00:00.000Z', lastAction: 'DELETED' },
        },
      ],
      nextCursor: undefined,
    })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result.status).toBe('ok')
    expect(mocks.upsertSyncDiffSummary).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({
        missingInNangoCount: 0,
        missingInShadowCount: 0,
        fieldMismatchCount: 0,
      }),
    )
  })

  it('does not flag missing_in_nango when a nango record was deleted but shadow still has it', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue([
      {
        type: 'issues-comment',
        sourceId: 'issue-1',
        occurredAt: '2026-09-13T12:00:00.000Z',
        data: { type: 'issues-comment', sourceId: 'issue-1', body: 'shadow body' },
      },
    ])
    mocks.getNangoCloudRecords.mockResolvedValue({
      records: [
        {
          timestamp: new Date('2026-09-13T12:00:00.000Z').getTime(),
          activity: { type: 'issues-comment', sourceId: 'issue-1', body: 'nango body' },
          metadata: { lastModifiedAt: '2026-09-13T12:00:00.000Z', lastAction: 'DELETED' },
        },
      ],
      nextCursor: undefined,
    })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result.status).toBe('ok')
    expect(mocks.upsertSyncDiffSummary).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ missingInNangoCount: 0 }),
    )
  })

  it('flags units with an unrecognized syncName instead of silently reporting a clean diff', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [{ ...UNIT, syncName: 'some-new-unmapped-sync' }],
    })

    expect(result.status).toBe('ok')
    expect(mocks.getShadowRecordsInWindow).not.toHaveBeenCalled()
    expect(mocks.upsertSyncDiffSummary).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ unsupportedSyncCount: 1 }),
    )
    expect(mocks.pruneMatchingShadowRecords).not.toHaveBeenCalled()
  })

  it('throws when a per-channel fetch fails, so Temporal can retry the activity', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockRejectedValue(new Error('db exploded'))

    await expect(
      runShadowDiffForChannel({
        channelName: UNIT.channelName,
        integrationId: UNIT.integrationId,
        units: [UNIT],
      }),
    ).rejects.toThrow('db exploded')
  })

  it('skips units that already have a summary for the day, to stay idempotent under Temporal retries', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getUnitIdsWithSummary.mockResolvedValue(new Set([UNIT.id]))

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result.status).toBe('ok')
    expect(mocks.getShadowRecordsInWindow).not.toHaveBeenCalled()
    expect(mocks.upsertSyncDiffSummary).not.toHaveBeenCalled()
    expect(mocks.pruneMatchingShadowRecords).not.toHaveBeenCalled()
  })
})
