import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { diffUnit } from './shadowDiffUnit'

const mocks = vi.hoisted(() => ({
  getShadowRecordsInWindow: vi.fn(),
  initNangoCloudClient: vi.fn(),
  getNangoCloudRecords: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  getShadowRecordsInWindow: mocks.getShadowRecordsInWindow,
}))

vi.mock('@crowd/nango', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/nango')>()),
  NangoIntegration: { GITHUB: 'github' },
  NangoMetadataLastAction: { ADDED: 'ADDED', UPDATED: 'UPDATED', DELETED: 'DELETED' },
  initNangoCloudClient: mocks.initNangoCloudClient,
  getNangoCloudRecords: mocks.getNangoCloudRecords,
}))

const qx = {} as QueryExecutor
const UNIT = {
  id: 'unit-1',
  integrationId: 'integration-1',
  channelName: 'https://github.com/twentyhq/twenty',
  syncName: 'pull-requests',
}
const WINDOW_START = new Date('2026-10-17T00:00:00.000Z')
const WINDOW_END = new Date('2026-10-18T00:00:00.000Z')
const IN_WINDOW = '2026-10-17T12:00:00.000Z'

function nangoRecord(activity: Record<string, unknown>, id?: string) {
  return {
    id,
    timestamp: new Date(IN_WINDOW).getTime(),
    activity,
    metadata: { lastModifiedAt: IN_WINDOW, lastAction: 'ADDED' },
  }
}

describe('diffUnit', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('stamps the nango record id and sourceParentId on missing_in_shadow mismatches only', async () => {
    mocks.getShadowRecordsInWindow.mockResolvedValue([
      {
        type: 'pull_request-closed',
        sourceId: 'gen-CE_PR_1_bob_2026-10-17T12:00:00.000Z',
        occurredAt: IN_WINDOW,
        data: { type: 'pull_request-closed', sourceId: 'gen-CE_PR_1_bob_2026-10-17T12:00:00.000Z' },
      },
    ])
    mocks.getNangoCloudRecords.mockResolvedValue({
      records: [
        nangoRecord(
          {
            type: 'pull_request-reviewed',
            sourceId: 'gen-PRR_PR_1_alice_2026-10-17T12:00:00.000Z',
            sourceParentId: 'PR_1',
          },
          'PRR_x-reviewed',
        ),
        nangoRecord({ type: 'pull_request-opened', sourceId: 'PR_1' }, 'PR_1-opened'),
        nangoRecord({
          type: 'pull_request-merged',
          sourceId: 'gen-ME_PR_2_carol_2026-10-17T12:00:00.000Z',
        }),
      ],
      nextCursor: undefined,
    })

    const { mismatches } = await diffUnit(qx, UNIT, 'conn-1', WINDOW_START, WINDOW_END)

    expect(mismatches).toEqual([
      {
        sourceId: 'gen-CE_PR_1_bob_2026-10-17T12:00:00.000Z',
        type: 'pull_request-closed',
        kind: 'missing_in_nango',
        severity: 'high',
        syncName: 'pull-requests',
      },
      {
        sourceId: 'gen-PRR_PR_1_alice_2026-10-17T12:00:00.000Z',
        type: 'pull_request-reviewed',
        kind: 'missing_in_shadow',
        severity: 'high',
        syncName: 'pull-requests',
        nangoRecordId: 'PRR_x-reviewed',
        sourceParentId: 'PR_1',
      },
      {
        sourceId: 'PR_1',
        type: 'pull_request-opened',
        kind: 'missing_in_shadow',
        severity: 'high',
        syncName: 'pull-requests',
        nangoRecordId: 'PR_1-opened',
      },
      {
        sourceId: 'gen-ME_PR_2_carol_2026-10-17T12:00:00.000Z',
        type: 'pull_request-merged',
        kind: 'missing_in_shadow',
        severity: 'high',
        syncName: 'pull-requests',
      },
    ])
    expect(Object.keys(mismatches[0])).not.toContain('nangoRecordId')
    expect(Object.keys(mismatches[3])).not.toContain('nangoRecordId')
  })
})
