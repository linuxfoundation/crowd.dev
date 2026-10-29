import { beforeEach, describe, expect, it, vi } from 'vitest'

import { validateUnit } from './completenessActivities'

const mocks = vi.hoisted(() => ({
  upsertValidationResult: vi.fn(),
  listValidationUnitsPage: vi.fn(),
  fetchIntegrationById: vi.fn(),
  countTinybirdActivities: vi.fn(),
  findSegmentsForRepos: vi.fn(),
  createPlatformHttp: vi.fn(),
  getManifest: vi.fn(),
  selectCounters: vi.fn(),
  heartbeat: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/activity', () => ({
  Context: { current: () => ({ heartbeat: mocks.heartbeat }) },
}))
vi.mock('../main', () => ({ svc: { postgres: { writer: {} }, log: mocks.log } }))
vi.mock('@crowd/logging', () => ({ getChildLogger: () => mocks.log }))
vi.mock('@crowd/database', () => ({ TinybirdClient: class {} }))
vi.mock('@crowd/connectors', () => ({
  getManifest: mocks.getManifest,
  selectCounters: mocks.selectCounters,
}))
vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({ dbStoreQx: vi.fn(() => 'qx') }))
vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  listValidationUnitsPage: mocks.listValidationUnitsPage,
  upsertValidationResult: mocks.upsertValidationResult,
}))
vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  fetchIntegrationById: mocks.fetchIntegrationById,
}))
vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  findSegmentsForRepos: mocks.findSegmentsForRepos,
}))
vi.mock('../platformHttp', () => ({ createPlatformHttp: mocks.createPlatformHttp }))
vi.mock('../tinybirdActivityCount', () => ({
  countTinybirdActivities: mocks.countTinybirdActivities,
}))

const unit = {
  id: 'u1',
  integrationId: 'i1',
  platform: 'github',
  channelId: 'c1',
  channelName: 'acme/repo',
  syncName: 'issues',
  validationCounterKeys: null,
  validationEnabled: true,
}
const window = { kind: 'all-time' as const }

describe('validateUnit', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.getManifest.mockReturnValue({})
    mocks.fetchIntegrationById.mockResolvedValue({ segmentId: 'seg' })
    mocks.countTinybirdActivities.mockResolvedValue(7)
    mocks.findSegmentsForRepos.mockResolvedValue([{ segmentId: 'repo-seg' }])
  })

  it('stores source and tinybird counts for every selected counter', async () => {
    mocks.selectCounters.mockReturnValue([
      { key: 'a', activityTypes: ['t1'], count: vi.fn().mockResolvedValue(5) },
      { key: 'b', activityTypes: ['t2'], count: vi.fn().mockResolvedValue(6) },
    ])
    const result = await validateUnit({ unit, window, counterKeys: null })
    expect(result).toEqual({ unitId: 'u1', written: 2, failures: [], noCounters: false })
    expect(mocks.upsertValidationResult).toHaveBeenCalledWith('qx', {
      unitId: 'u1',
      counterKey: 'a',
      window,
      sourceCount: 5,
      tinybirdCount: 7,
    })
    expect(mocks.countTinybirdActivities).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        segmentId: 'repo-seg',
        channel: 'acme/repo',
        activityTypes: ['t2'],
      }),
    )
  })

  it('records a failure for a throwing counter and still stores the others', async () => {
    mocks.selectCounters.mockReturnValue([
      { key: 'a', activityTypes: [], count: vi.fn().mockRejectedValue(new Error('boom')) },
      { key: 'b', activityTypes: [], count: vi.fn().mockResolvedValue(1) },
    ])
    const result = await validateUnit({ unit, window, counterKeys: null })
    expect(result.written).toBe(1)
    expect(result.failures).toEqual([{ unitId: 'u1', counterKey: 'a', errorMessage: 'boom' }])
    expect(mocks.upsertValidationResult).toHaveBeenCalledTimes(1)
    expect(mocks.upsertValidationResult).toHaveBeenCalledWith(
      'qx',
      expect.objectContaining({ counterKey: 'b' }),
    )
  })

  it('reports noCounters without writing when the sync has no counters', async () => {
    mocks.selectCounters.mockReturnValue([])
    const result = await validateUnit({ unit, window, counterKeys: null })
    expect(result).toEqual({ unitId: 'u1', written: 0, failures: [], noCounters: true })
    expect(mocks.upsertValidationResult).not.toHaveBeenCalled()
    expect(mocks.log.info).toHaveBeenCalledWith(
      { event: 'completeness_validation_no_counters' },
      expect.any(String),
    )
  })
})
