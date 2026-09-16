import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

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
  NangoMetadataLastAction: { ADDED: 'ADDED', UPDATED: 'UPDATED', DELETED: 'DELETED' },
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
      mismatches: [],
      totalMismatchCount: 0,
      syncSummaries: [],
    })
    expect(mocks.getShadowRecordsInWindow).not.toHaveBeenCalled()
  })

  it('reports mismatches found between shadow and nango records for the window', async () => {
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

    expect(result.status).toBe('ok')
    expect(result.mismatches).toHaveLength(1)
    expect(result.totalMismatchCount).toBe(1)
    expect(result.mismatches[0]).toMatchObject({
      sourceId: 'issue-1',
      kind: 'field_mismatch',
      syncName: 'issues',
    })
    expect(result.syncSummaries).toEqual([
      {
        syncName: 'issues',
        counts: {
          missing_in_nango: 0,
          missing_in_shadow: 0,
          field_mismatch: 1,
          unsupported_sync: 0,
        },
      },
    ])
    expect(mocks.initNangoCloudClient).toHaveBeenCalled()
    expect(getNangoCloudRecords).toHaveBeenCalledWith(
      'github',
      'conn-1',
      'GithubIssue',
      undefined,
      undefined,
      '2026-09-13T00:00:00.000Z',
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
    expect(result.mismatches).toHaveLength(0)
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
    expect(result.mismatches).toHaveLength(0)
  })

  it('caps reported mismatches at 50 while preserving the true total count', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })
    mocks.getShadowRecordsInWindow.mockResolvedValue(
      Array.from({ length: 60 }, (_, i) => ({
        type: 'issues-comment',
        sourceId: `issue-${i}`,
        occurredAt: '2026-09-13T12:00:00.000Z',
        data: { type: 'issues-comment', sourceId: `issue-${i}` },
      })),
    )
    mocks.getNangoCloudRecords.mockResolvedValue({ records: [], nextCursor: undefined })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [UNIT],
    })

    expect(result.status).toBe('ok')
    expect(result.mismatches).toHaveLength(50)
    expect(result.totalMismatchCount).toBe(60)
  })

  it('flags units with an unrecognized syncName instead of silently reporting a clean diff', async () => {
    mocks.getNangoMappingForRepo.mockResolvedValue({ connectionId: 'conn-1' })

    const result = await runShadowDiffForChannel({
      channelName: UNIT.channelName,
      integrationId: UNIT.integrationId,
      units: [{ ...UNIT, syncName: 'some-new-unmapped-sync' }],
    })

    expect(result.status).toBe('ok')
    expect(result.mismatches).toEqual([
      {
        sourceId: UNIT.id,
        type: 'some-new-unmapped-sync',
        kind: 'unsupported_sync',
        severity: 'high',
        syncName: 'some-new-unmapped-sync',
      },
    ])
    expect(result.syncSummaries).toEqual([
      {
        syncName: 'some-new-unmapped-sync',
        counts: {
          missing_in_nango: 0,
          missing_in_shadow: 0,
          field_mismatch: 0,
          unsupported_sync: 1,
        },
      },
    ])
    expect(mocks.getShadowRecordsInWindow).not.toHaveBeenCalled()
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
})

describe('reportShadowDiffResults', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.sendSlackNotificationAsync.mockResolvedValue(true)
  })

  it('sends one Slack alert per channel that has mismatches or an error, and skips healthy channels', async () => {
    await reportShadowDiffResults([
      {
        channelName: 'healthy-repo',
        integrationId: 'integration-1',
        status: 'ok',
        mismatches: [],
        totalMismatchCount: 0,
        syncSummaries: [],
      },
      {
        channelName: 'mismatched-repo',
        integrationId: 'integration-1',
        status: 'ok',
        mismatches: [
          {
            sourceId: 'a',
            type: 'issue',
            kind: 'missing_in_nango',
            severity: 'high',
            syncName: 'issues',
          },
        ],
        totalMismatchCount: 1,
        syncSummaries: [
          {
            syncName: 'issues',
            counts: {
              missing_in_nango: 1,
              missing_in_shadow: 0,
              field_mismatch: 0,
              unsupported_sync: 0,
            },
          },
        ],
      },
      {
        channelName: 'unmapped-repo',
        integrationId: 'integration-1',
        status: 'mapping_missing',
        mismatches: [],
        totalMismatchCount: 0,
        syncSummaries: [],
      },
      {
        channelName: 'broken-repo',
        integrationId: 'integration-1',
        status: 'error',
        mismatches: [],
        totalMismatchCount: 0,
        syncSummaries: [],
        errorMessage: 'boom',
      },
    ])

    expect(sendSlackNotificationAsync).toHaveBeenCalledTimes(3)
  })

  it('identifies the integration in the alert title so shared repos are distinguishable', async () => {
    await reportShadowDiffResults([
      {
        channelName: 'shared-repo',
        integrationId: 'integration-1',
        status: 'mapping_missing',
        mismatches: [],
        totalMismatchCount: 0,
        syncSummaries: [],
      },
    ])

    expect(sendSlackNotificationAsync).toHaveBeenCalledWith(
      'CDP_INTEGRATIONS_ALERTS',
      'WARNING_PROPAGATOR',
      'Shadow diff: no nango mapping for shared-repo (integration integration-1)',
      expect.any(String),
    )
  })

  it('truncates the Slack title so long channel names cannot exceed the header block limit even with the persona icon prefixed', async () => {
    const longChannelName = `https://github.com/${'a'.repeat(150)}/${'b'.repeat(150)}`

    await reportShadowDiffResults([
      {
        channelName: longChannelName,
        integrationId: 'integration-1',
        status: 'mapping_missing',
        mismatches: [],
        totalMismatchCount: 0,
        syncSummaries: [],
      },
    ])

    expect(sendSlackNotificationAsync).toHaveBeenCalledTimes(1)
    const [, , title] = vi.mocked(sendSlackNotificationAsync).mock.calls[0]
    const SLACK_HEADER_MAX_LENGTH = 150
    const LONGEST_PERSONA_ICON_PREFIX = ':rotating_light: '
    expect(title.length + LONGEST_PERSONA_ICON_PREFIX.length).toBeLessThanOrEqual(
      SLACK_HEADER_MAX_LENGTH,
    )
  })

  it('notes how many mismatches were truncated when totalMismatchCount exceeds the reported list', async () => {
    await reportShadowDiffResults([
      {
        channelName: 'noisy-repo',
        integrationId: 'integration-1',
        status: 'ok',
        mismatches: [
          {
            sourceId: 'a',
            type: 'issue',
            kind: 'missing_in_nango',
            severity: 'high',
            syncName: 'issues',
          },
        ],
        totalMismatchCount: 3,
        syncSummaries: [
          {
            syncName: 'issues',
            counts: {
              missing_in_nango: 3,
              missing_in_shadow: 0,
              field_mismatch: 0,
              unsupported_sync: 0,
            },
          },
        ],
      },
    ])

    expect(sendSlackNotificationAsync).toHaveBeenCalledWith(
      'CDP_INTEGRATIONS_ALERTS',
      'WARNING_PROPAGATOR',
      'Shadow diff mismatches for noisy-repo (integration integration-1)',
      expect.stringContaining('2 more mismatch(es) not shown overall'),
    )
  })

  it('groups the report body by sync name with a counts table followed by per-sync diff details', async () => {
    await reportShadowDiffResults([
      {
        channelName: 'linuxfoundation/insights',
        integrationId: 'integration-1',
        status: 'ok',
        mismatches: [
          {
            sourceId: 'i-1',
            type: 'issue',
            kind: 'missing_in_nango',
            severity: 'high',
            syncName: 'issues',
          },
          {
            sourceId: 'i-2',
            type: 'issue',
            kind: 'missing_in_nango',
            severity: 'high',
            syncName: 'issues',
          },
          {
            sourceId: 'c-1',
            type: 'issues-comment',
            kind: 'missing_in_nango',
            severity: 'high',
            syncName: 'issue-comments',
          },
        ],
        totalMismatchCount: 3,
        syncSummaries: [
          {
            syncName: 'discussions',
            counts: {
              missing_in_nango: 0,
              missing_in_shadow: 0,
              field_mismatch: 0,
              unsupported_sync: 0,
            },
          },
          {
            syncName: 'issues',
            counts: {
              missing_in_nango: 2,
              missing_in_shadow: 0,
              field_mismatch: 0,
              unsupported_sync: 0,
            },
          },
          {
            syncName: 'issue-comments',
            counts: {
              missing_in_nango: 1,
              missing_in_shadow: 0,
              field_mismatch: 0,
              unsupported_sync: 0,
            },
          },
        ],
      },
    ])

    const [, , , body] = vi.mocked(sendSlackNotificationAsync).mock.calls[0]
    const bodyText = String(body)

    expect(bodyText).toContain('discussions')
    expect(bodyText).toContain('issues')
    expect(bodyText).toContain('issue-comments')
    expect(bodyText.indexOf('```')).toBeLessThan(bodyText.indexOf('*issues*'))
    expect(bodyText.indexOf('*issues*')).toBeLessThan(bodyText.indexOf('*issue-comments*'))
    expect(bodyText).toContain('i-1')
    expect(bodyText).toContain('i-2')
    expect(bodyText).toContain('c-1')
  })

  it('throws when a Slack alert fails to deliver, so Temporal retries instead of silently dropping the alert', async () => {
    mocks.sendSlackNotificationAsync.mockResolvedValue(false)

    await expect(
      reportShadowDiffResults([
        {
          channelName: 'noisy-repo',
          integrationId: 'integration-1',
          status: 'ok',
          mismatches: [
            {
              sourceId: 'a',
              type: 'issue',
              kind: 'missing_in_nango',
              severity: 'high',
              syncName: 'issues',
            },
          ],
          totalMismatchCount: 1,
          syncSummaries: [
            {
              syncName: 'issues',
              counts: {
                missing_in_nango: 1,
                missing_in_shadow: 0,
                field_mismatch: 0,
                unsupported_sync: 0,
              },
            },
          ],
        },
      ]),
    ).rejects.toThrow('noisy-repo (integration integration-1)')
  })
})
