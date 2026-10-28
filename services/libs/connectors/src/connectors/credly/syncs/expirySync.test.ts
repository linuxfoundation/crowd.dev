import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { ICredlyBadgeRow } from '@crowd/data-access-layer/src/credly/badges'
import { CredlyActivityType } from '@crowd/integrations/src/integrations/credly/types'
import type { Logger } from '@crowd/logging'

import type { Credential, SyncContext } from '../../../types'
import type { CredlyDeps } from '../deps'
import { EXPIRY_BATCH_SIZE, runExpirySync } from './expirySync'

const LOG = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger
const CREDENTIAL: Credential = {
  platform: 'credly',
  kind: 'credly-oauth-client',
  data: { clientId: 'id', clientSecret: 'secret', organizationId: 'org' },
}

function dueRow(badgeId: string, overrides: Partial<ICredlyBadgeRow> = {}): ICredlyBadgeRow {
  return {
    badgeId,
    badgeTemplateId: 'tpl-cka',
    badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
    badgeTemplateUrl: 'https://www.credly.com/org/the-linux-foundation/badge/cka',
    state: 'accepted',
    stateUpdatedAt: '2022-03-01 10:00:00+00',
    issuedAt: '2022-02-28 00:00:00+00',
    expiresAt: '2025-02-28 00:00:00+00',
    replacementBadgeId: null,
    replacesBadgeId: null,
    segmentId: 'seg-k8s',
    integrationId: 'int-k8s',
    recipientEmail: 'maria.rossi@example.org',
    issuedTo: 'Maria Rossi',
    expiredEmittedAt: null,
    firstSeenAt: '2026-10-28 11:36:02+00',
    lastSeenAt: '2026-10-28 11:36:02+00',
    ...overrides,
  }
}

function makeCtx(batches: ICredlyBadgeRow[][], emitEnabled = true, hasRunBudget = () => true) {
  const emits: { records: unknown[]; segmentId?: string; integrationId?: string }[] = []
  const marked: string[][] = []
  const queue = [...batches]
  const deps = {
    listDueExpiries: vi.fn(async () => queue.shift() ?? []),
    markExpiriesEmitted: vi.fn(async (ids: string[]) => {
      marked.push(ids)
      return ids.length
    }),
  } as unknown as CredlyDeps
  const ctx: SyncContext<CredlyDeps> = {
    channel: { channelId: 'org', channelName: 'org' },
    integrationId: 'anchor',
    watermark: null,
    emitEnabled,
    emit: async (records, overrides) => {
      emits.push({ records, ...overrides })
    },
    commitWatermark: async () => undefined,
    hasRunBudget,
    http: {} as SyncContext['http'],
    log: LOG,
    getCredential: async () => CREDENTIAL,
    deps,
  }
  return { ctx, emits, marked, deps }
}

describe('runExpirySync', () => {
  beforeEach(() => vi.clearAllMocks())

  it('emits one expired-badge per due badge on its stored segment and marks it', async () => {
    const { ctx, emits, marked } = makeCtx([
      [
        dueRow('b-1'),
        dueRow('b-2', { segmentId: 'seg-istio', integrationId: 'int-istio' }),
        dueRow('b-3'),
      ],
    ])

    const outcome = await runExpirySync(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(
      emits.map(({ records, segmentId, integrationId }) => ({
        ids: records.map((r) => (r as { sourceId: string }).sourceId),
        types: [...new Set(records.map((r) => (r as { type: string }).type))],
        segmentId,
        integrationId,
      })),
    ).toEqual([
      {
        ids: ['b-1', 'b-3'],
        types: [CredlyActivityType.EXPIRED_BADGE],
        segmentId: 'seg-k8s',
        integrationId: 'int-k8s',
      },
      {
        ids: ['b-2'],
        types: [CredlyActivityType.EXPIRED_BADGE],
        segmentId: 'seg-istio',
        integrationId: 'int-istio',
      },
    ])
    expect(marked).toEqual([['b-1', 'b-3'], ['b-2']])
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'credly_expiry_sync_run', due: 3, emitted: 3, marked: 3 }),
      expect.any(String),
    )
  })

  it('builds the activity from the stored row: expiry date, template, earner of the first sight', async () => {
    const { ctx, emits } = makeCtx([[dueRow('b-1')]])

    await runExpirySync(ctx)

    const [activity] = emits[0].records as Record<string, unknown>[]
    expect(activity).toMatchObject({
      type: CredlyActivityType.EXPIRED_BADGE,
      timestamp: '2025-02-28T00:00:00.000Z',
      sourceId: 'b-1',
      title: 'CKA: Certified Kubernetes Administrator',
      url: 'https://www.credly.com/badges/b-1',
      channel: 'tpl-cka',
      attributes: expect.objectContaining({
        badgeTemplateId: 'tpl-cka',
        state: 'accepted',
        expiresAt: '2025-02-28T00:00:00.000Z',
      }),
      member: {
        displayName: 'Maria Rossi',
        identities: [
          expect.objectContaining({ type: 'username', value: 'maria.rossi@example.org' }),
          expect.objectContaining({ type: 'email', value: 'maria.rossi@example.org' }),
        ],
      },
    })
  })

  it('drains the queue in batches and stops when a batch comes back short', async () => {
    const full = Array.from({ length: EXPIRY_BATCH_SIZE }, (_, i) => dueRow(`full-${i}`))
    const { ctx, marked, deps } = makeCtx([full, [dueRow('last')], [dueRow('never')]])

    await runExpirySync(ctx)

    expect(deps.listDueExpiries).toHaveBeenCalledTimes(2)
    expect(marked.map((ids) => ids.length)).toEqual([EXPIRY_BATCH_SIZE, 1])
  })

  it('defers instead of waiting a day when the budget runs out with rows still due', async () => {
    const full = Array.from({ length: EXPIRY_BATCH_SIZE }, (_, i) => dueRow(`full-${i}`))
    let calls = 0
    const { ctx, marked } = makeCtx(
      [full, full.map((row) => dueRow(`${row.badgeId}-b`))],
      true,
      () => calls++ < 1,
    )

    const outcome = await runExpirySync(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(marked).toHaveLength(1)
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'credly_expiry_sync_run', drained: false }),
      expect.any(String),
    )
  })

  it('walks the queue with a cursor so rows it could not mark do not block the rest', async () => {
    const full = Array.from({ length: EXPIRY_BATCH_SIZE }, (_, i) =>
      dueRow(`full-${String(i).padStart(3, '0')}`, { expiresAt: '2025-01-01 00:00:00+00' }),
    )
    const { ctx, deps } = makeCtx([full, [dueRow('after')], []])
    vi.mocked(deps.markExpiriesEmitted).mockResolvedValue(0)

    await runExpirySync(ctx)

    expect(deps.listDueExpiries).toHaveBeenCalledTimes(2)
    expect(vi.mocked(deps.listDueExpiries).mock.calls[1][2]).toEqual({
      expiresAt: '2025-01-01 00:00:00+00',
      badgeId: 'full-499',
    })
  })

  it('skips a row it cannot map and still marks the rest', async () => {
    const { ctx, emits, marked } = makeCtx([[dueRow('bad', { expiresAt: null }), dueRow('good')]])

    await runExpirySync(ctx)

    expect(emits).toHaveLength(1)
    expect(marked).toEqual([['good']])
    expect(LOG.warn).toHaveBeenCalledWith(
      { badgeId: 'bad', errMsg: expect.any(String) },
      'skipping credly expiry: stored badge cannot be mapped',
    )
  })

  it('emits but marks nothing during a shadow run, and reads the queue once', async () => {
    const { ctx, emits, marked, deps } = makeCtx([[dueRow('b-1')], [dueRow('b-1')]], false)

    await runExpirySync(ctx)

    expect(emits).toHaveLength(1)
    expect(marked).toEqual([])
    expect(deps.listDueExpiries).toHaveBeenCalledTimes(1)
  })
})
