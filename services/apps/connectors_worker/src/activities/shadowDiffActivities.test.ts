import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getNangoCloudRecords } from '@crowd/nango'
import { sendSlackNotificationAsync } from '@crowd/slack'

import {
  listShadowDiffChannels,
  reportShadowDiffResults,
  runShadowDiffForChannel,
} from './shadowDiffActivities'

const mocks = vi.hoisted(() => ({
  listShadowDiffUnits: vi.fn(),
  getNangoMappingForRepo: vi.fn(),
  getShadowRecordsInWindow: vi.fn(),
  initNangoCloudClient: vi.fn(),
  getNangoCloudRecords: vi.fn(),
  sendSlackNotificationAsync: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  },
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  dbStoreQx: vi.fn(() => ({})),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  listShadowDiffUnits: mocks.listShadowDiffUnits,
  getShadowRecordsInWindow: mocks.getShadowRecordsInWindow,
}))

vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  getNangoMappingForRepo: mocks.getNangoMappingForRepo,
}))

vi.mock('@crowd/nango', () => ({
  NangoIntegration: { GITHUB: 'github' },
  initNangoCloudClient: mocks.initNangoCloudClient,
  getNangoCloudRecords: mocks.getNangoCloudRecords,
}))

vi.mock('@crowd/slack', () => ({
  sendSlackNotificationAsync: mocks.sendSlackNotificationAsync,
  SlackChannel: { CDP_INTEGRATIONS_ALERTS: 'CDP_INTEGRATIONS_ALERTS' },
  SlackPersona: { WARNING_PROPAGATOR: 'WARNING_PROPAGATOR', ERROR_REPORTER: 'ERROR_REPORTER' },
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
})

describe('runShadowDiffForChannel', () => {
  beforeEach(() => {
    vi.clearAllMocks()
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
      status: 'mapping_missing',
      mismatches: [],
    })
    expect(mocks.getShadowRecordsInWindow).not.toHaveBeenCalled()
  })

  it('reports mismatches found between shadow and nango records for the window', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-15T12:00:00.000Z'))

    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue([
      {
        type: 'issues-comment',
        sourceId: 'issue-1',
        occurredAt: '2026-09-14T12:00:00.000Z',
        data: { type: 'issues-comment', sourceId: 'issue-1', body: 'shadow body' },
      },
    ])
    mocks.getNangoCloudRecords.mockResolvedValue({
      records: [
        {
          timestamp: new Date('2026-09-14T12:00:00.000Z').getTime(),
          activity: { type: 'issues-comment', sourceId: 'issue-1', body: 'nango body' },
          metadata: { lastModifiedAt: '2026-09-14T12:00:00.000Z' },
        },
      ],
      nextCursor: undefined,
    })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    vi.useRealTimers()

    expect(result.status).toBe('ok')
    expect(result.mismatches).toHaveLength(1)
    expect(result.mismatches[0]).toMatchObject({
      sourceId: 'issue-1',
      kind: 'field_mismatch',
    })
    expect(mocks.initNangoCloudClient).toHaveBeenCalled()
    expect(getNangoCloudRecords).toHaveBeenCalledWith(
      'github',
      'conn-1',
      'GithubIssue',
      undefined,
      undefined,
      '2026-09-14T00:00:00.000Z',
    )
  })

  it('reports status error and does not throw when a per-channel fetch fails', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockRejectedValue(new Error('db exploded'))

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result.status).toBe('error')
    expect(result.errorMessage).toContain('db exploded')
  })
})

describe('reportShadowDiffResults', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('sends one Slack alert per channel that has mismatches or an error, and skips healthy channels', async () => {
    await reportShadowDiffResults([
      { channelName: 'healthy-repo', status: 'ok', mismatches: [] },
      {
        channelName: 'mismatched-repo',
        status: 'ok',
        mismatches: [{ sourceId: 'a', type: 'issue', kind: 'missing_in_nango', severity: 'high' }],
      },
      { channelName: 'unmapped-repo', status: 'mapping_missing', mismatches: [] },
      { channelName: 'broken-repo', status: 'error', mismatches: [], errorMessage: 'boom' },
    ])

    expect(sendSlackNotificationAsync).toHaveBeenCalledTimes(3)
  })
})
