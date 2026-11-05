import { beforeEach, describe, expect, it, vi } from 'vitest'

import { runOctolensDiscovery } from './octolensDiscoveryActivities'

const calls: string[] = []
const record =
  <T>(name: string, value: T) =>
  async (...args: unknown[]) => {
    calls.push(`${name}${args.length > 1 ? ` ${JSON.stringify(args.slice(1))}` : ''}`)
    return value
  }

const mocks = vi.hoisted(() => ({
  findOrCreateOctolensAnchorIntegration: vi.fn(),
  reconcileOctolensKeywordMappings: vi.fn(),
  upsertSyncUnits: vi.fn(),
  reviveDeadLetteredSyncUnits: vi.fn(),
  getCredential: vi.fn(),
  discover: vi.fn(),
  runMappingDiscovery: vi.fn(),
  isOctolensCredentialConfigured: vi.fn(),
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

vi.mock('@crowd/data-access-layer/src/octolens', () => ({
  findOrCreateOctolensAnchorIntegration: mocks.findOrCreateOctolensAnchorIntegration,
  reconcileOctolensKeywordMappings: mocks.reconcileOctolensKeywordMappings,
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  upsertSyncUnits: mocks.upsertSyncUnits,
  reviveDeadLetteredSyncUnits: mocks.reviveDeadLetteredSyncUnits,
}))

vi.mock('@crowd/connectors', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/connectors')>()),
  getCredential: mocks.getCredential,
  getManifest: () => ({ discover: mocks.discover }),
  isOctolensCredentialConfigured: mocks.isOctolensCredentialConfigured,
}))

vi.mock('@crowd/connectors/src/connectors/octolens/deps', () => ({
  resolveOctolensProjectSegmentId: vi.fn(),
}))

vi.mock('@crowd/connectors/src/connectors/octolens/mappingDiscovery', () => ({
  runMappingDiscovery: mocks.runMappingDiscovery,
}))

const MAPPINGS = [
  { keywordId: 1, keyword: 'ebpf', segmentId: 'segment-ebpf' },
  { keywordId: 3, keyword: 'valkey', segmentId: 'segment-valkey' },
]

beforeEach(() => {
  vi.clearAllMocks()
  calls.length = 0
  mocks.isOctolensCredentialConfigured.mockReturnValue(true)
  mocks.findOrCreateOctolensAnchorIntegration.mockImplementation(
    record('anchor', { id: 'anchor', created: true, restored: false }),
  )
  mocks.getCredential.mockResolvedValue({
    platform: 'octolens',
    kind: 'octolens-token',
    data: { token: 'token' },
  })
  mocks.discover.mockResolvedValue([
    { channelId: 'octolens-global', channelName: 'octolens-global' },
  ])
  mocks.upsertSyncUnits.mockImplementation(record('upsertSyncUnits', 1))
  mocks.reviveDeadLetteredSyncUnits.mockImplementation(record('revive', 0))
  mocks.reconcileOctolensKeywordMappings.mockImplementation(
    record('reconcile', { deactivated: 4, created: [], restored: [], softDeleted: [] }),
  )
  mocks.runMappingDiscovery.mockImplementation(
    async ({
      reconcileMappings,
    }: {
      reconcileMappings: (m: typeof MAPPINGS) => Promise<number>
    }) => {
      const deactivated = await reconcileMappings(MAPPINGS)
      return { complete: true, mappings: MAPPINGS.length, deactivated }
    },
  )
})

describe('runOctolensDiscovery', () => {
  it('ensures the anchor and its mention poll unit with emitEnabled, so mentions become activities', async () => {
    await runOctolensDiscovery()

    expect(mocks.getCredential).toHaveBeenCalledWith(expect.anything(), 'anchor')
    expect(mocks.upsertSyncUnits).toHaveBeenCalledWith(expect.anything(), [
      {
        integrationId: 'anchor',
        platform: 'octolens',
        channelId: 'octolens-global',
        channelName: 'octolens-global',
        syncName: 'mentionPoll',
        emitEnabled: true,
      },
    ])
  })

  it('reconciles the discovered mappings, then revives the mention poll', async () => {
    const result = await runOctolensDiscovery()

    expect(calls).toEqual([
      'anchor',
      'upsertSyncUnits [[{"integrationId":"anchor","platform":"octolens","channelId":"octolens-global","channelName":"octolens-global","syncName":"mentionPoll","emitEnabled":true}]]',
      `reconcile [${JSON.stringify(MAPPINGS)}]`,
      'revive ["anchor","mentionPoll"]',
    ])
    expect(result).toEqual({ complete: true, mappings: 2, deactivated: 4 })
  })

  it('does nothing at all when no Octolens token is configured', async () => {
    mocks.isOctolensCredentialConfigured.mockReturnValue(false)

    const result = await runOctolensDiscovery()

    expect(result).toEqual({ complete: false, mappings: 0, deactivated: 0 })
    expect(mocks.findOrCreateOctolensAnchorIntegration).not.toHaveBeenCalled()
    expect(mocks.upsertSyncUnits).not.toHaveBeenCalled()
    expect(mocks.runMappingDiscovery).not.toHaveBeenCalled()
  })

  it('does not revive the mention poll when Octolens rejects the token', async () => {
    mocks.runMappingDiscovery.mockRejectedValue(new Error('provider authentication failed'))

    await expect(runOctolensDiscovery()).rejects.toThrow('provider authentication failed')
    expect(mocks.reviveDeadLetteredSyncUnits).not.toHaveBeenCalled()
  })

  it('makes no mapping writes when discovery does not reconcile (incomplete snapshot)', async () => {
    mocks.runMappingDiscovery.mockResolvedValue({ complete: false, mappings: 0, deactivated: 0 })

    await runOctolensDiscovery()

    expect(mocks.reconcileOctolensKeywordMappings).not.toHaveBeenCalled()
  })
})
