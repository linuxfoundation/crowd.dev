import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { LIFECYCLE_STALE_LOCK_MS } from './githubLifecycle'
import { onboardCredlyTemplate } from './onboardCredlyTemplate'

const mocks = vi.hoisted(() => ({
  hasActiveSyncUnitsForChannelElsewhere: vi.fn(),
  listSyncUnitChannelNames: vi.fn(),
  lockSyncUnitChannel: vi.fn(),
  alignSyncUnitChannelName: vi.fn(),
  upsertSyncUnits: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  hasActiveSyncUnitsForChannelElsewhere: mocks.hasActiveSyncUnitsForChannelElsewhere,
  listSyncUnitChannelNames: mocks.listSyncUnitChannelNames,
  lockSyncUnitChannel: mocks.lockSyncUnitChannel,
  alignSyncUnitChannelName: mocks.alignSyncUnitChannelName,
  upsertSyncUnits: mocks.upsertSyncUnits,
}))

const txQx = { name: 'tx' } as unknown as QueryExecutor
const tx = vi.fn(async (fn: (inner: QueryExecutor) => Promise<unknown>) => fn(txQx))
const qx = { tx } as unknown as QueryExecutor
const backfill = {
  since: '2026-10-25T00:00:00.000Z',
  backfill: { floor: '2019-03-01T00:00:00.000Z' },
}

describe('onboardCredlyTemplate', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(false)
    mocks.listSyncUnitChannelNames.mockResolvedValue([])
  })

  it('creates a badgePoll unit, checking the channel lock and other integrations under credly only', async () => {
    await expect(onboardCredlyTemplate(qx, 'int-k8s', 'tpl-cka', 'CKA', backfill)).resolves.toEqual(
      { outcome: 'created' },
    )

    expect(mocks.lockSyncUnitChannel).toHaveBeenCalledWith(txQx, 'tpl-cka')
    expect(mocks.hasActiveSyncUnitsForChannelElsewhere).toHaveBeenCalledWith(
      txQx,
      'int-k8s',
      'tpl-cka',
      expect.any(Date),
      'credly',
    )
    const staleLockBefore: Date = mocks.hasActiveSyncUnitsForChannelElsewhere.mock.calls[0][3]
    expect(Date.now() - staleLockBefore.getTime()).toBeGreaterThanOrEqual(LIFECYCLE_STALE_LOCK_MS)
    expect(mocks.listSyncUnitChannelNames).toHaveBeenCalledWith(txQx, 'int-k8s', 'tpl-cka')
    expect(mocks.upsertSyncUnits).toHaveBeenCalledWith(txQx, [
      {
        integrationId: 'int-k8s',
        platform: 'credly',
        channelId: 'tpl-cka',
        channelName: 'CKA',
        syncName: 'badgePoll',
        emitEnabled: true,
        watermark: backfill,
      },
    ])
  })

  it('skips without creating a unit when another integration already owns the template', async () => {
    mocks.hasActiveSyncUnitsForChannelElsewhere.mockResolvedValue(true)

    await expect(onboardCredlyTemplate(qx, 'int-k8s', 'tpl-cka', 'CKA', backfill)).resolves.toEqual(
      {
        outcome: 'skipped',
        reason: 'owned_by_other_integration',
        message: 'units exist for CKA under another integration',
      },
    )
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
    expect(mocks.alignSyncUnitChannelName).not.toHaveBeenCalled()
  })

  it('aligns the stored channel name and skips when a unit already exists', async () => {
    mocks.listSyncUnitChannelNames.mockResolvedValue(['Old CKA Name'])

    await expect(onboardCredlyTemplate(qx, 'int-k8s', 'tpl-cka', 'CKA', backfill)).resolves.toEqual(
      {
        outcome: 'skipped',
        reason: 'already_present',
        message: 'unit already exists for CKA',
      },
    )
    expect(mocks.alignSyncUnitChannelName).toHaveBeenCalledWith(txQx, 'int-k8s', 'tpl-cka', 'CKA')
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
  })
})
