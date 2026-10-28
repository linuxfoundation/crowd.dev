import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import type { ICredlyBadgeInput } from './badges'
import {
  findCredlyBadges,
  listDueCredlyExpiries,
  markCredlyExpiriesEmitted,
  upsertCredlyBadges,
} from './badges'
import { findOrCreateCredlyIntegration } from './index'

const test = withQx(base)

async function project(qx: QueryExecutor): Promise<{ segmentId: string; integrationId: string }> {
  const segmentId = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.segments (id, slug, name, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), $(slug), $(slug), $(tenantId), NOW(), NOW())
    `,
    { id: segmentId, slug: `segment-${segmentId}`, tenantId: DEFAULT_TENANT_ID },
  )
  const { id: integrationId } = await findOrCreateCredlyIntegration(qx, segmentId)
  return { segmentId, integrationId }
}

function badge(
  badgeId: string,
  target: { segmentId: string; integrationId: string },
  overrides: Partial<ICredlyBadgeInput> = {},
): ICredlyBadgeInput {
  return {
    badgeId,
    badgeTemplateId: 'tpl-cka',
    badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
    badgeTemplateUrl: 'https://www.credly.com/org/the-linux-foundation/badge/cka',
    state: 'accepted',
    stateUpdatedAt: '2026-10-20T10:00:00.000Z',
    issuedAt: '2026-10-20T00:00:00.000Z',
    expiresAt: null,
    replacementBadgeId: null,
    replacesBadgeId: null,
    recipientEmail: 'maria@example.org',
    issuedTo: 'Maria',
    ...target,
    ...overrides,
  }
}

async function readBadge(
  qx: QueryExecutor,
  badgeId: string,
): Promise<{ state: string; segmentId: string; updatedAt: string; lastSeenAt: string }> {
  return qx.selectOne(
    `SELECT state, "segmentId", "updatedAt", "lastSeenAt" FROM integration."credlyBadges" WHERE "badgeId" = $(badgeId)`,
    { badgeId },
  )
}

const BACKDATED = '2020-01-01T00:00:00Z'

async function backdate(qx: QueryExecutor, badgeId: string): Promise<void> {
  await qx.result(
    `UPDATE integration."credlyBadges" SET "updatedAt" = $(backdated), "lastSeenAt" = $(backdated) WHERE "badgeId" = $(badgeId)`,
    { badgeId, backdated: BACKDATED },
  )
}

const millis = (timestamp: string | Date) => new Date(timestamp).getTime()

describe('credly badges', () => {
  test('upsert stores a badge once and a repeat with the same state only moves lastSeenAt', async ({
    qx,
  }) => {
    const target = await project(qx)
    await upsertCredlyBadges(qx, [badge('b-1', target)])
    await backdate(qx, 'b-1')

    await upsertCredlyBadges(qx, [badge('b-1', target)])

    const stored = await readBadge(qx, 'b-1')
    expect(stored.state).toBe('accepted')
    expect(millis(stored.updatedAt)).toBe(millis(BACKDATED))
    expect(millis(stored.lastSeenAt)).toBeGreaterThan(millis(BACKDATED))
  })

  test('a state change is recorded, the segment of the first sight is kept', async ({ qx }) => {
    const first = await project(qx)
    const second = await project(qx)
    await upsertCredlyBadges(qx, [badge('b-2', first, { state: 'pending' })])
    await backdate(qx, 'b-2')

    await upsertCredlyBadges(qx, [
      badge('b-2', second, {
        state: 'replaced',
        stateUpdatedAt: '2026-10-25T10:00:00.000Z',
        replacementBadgeId: 'b-3',
      }),
    ])

    const stored = await readBadge(qx, 'b-2')
    expect(stored.state).toBe('replaced')
    expect(stored.segmentId).toBe(first.segmentId)
    expect(millis(stored.updatedAt)).toBeGreaterThan(millis(BACKDATED))
    const [row] = await findCredlyBadges(qx, ['b-2', 'b-unknown'])
    expect(row).toMatchObject({
      badgeId: 'b-2',
      replacementBadgeId: 'b-3',
      integrationId: first.integrationId,
      badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
    })
    expect(await findCredlyBadges(qx, [])).toEqual([])
  })

  test('a page with the same badge twice keeps the last version', async ({ qx }) => {
    const target = await project(qx)

    await upsertCredlyBadges(qx, [
      badge('b-4', target, { state: 'pending' }),
      badge('b-4', target, { state: 'accepted' }),
    ])

    expect((await readBadge(qx, 'b-4')).state).toBe('accepted')
  })

  test('due expiries are the accepted badges past their date that were not emitted yet', async ({
    qx,
  }) => {
    const target = await project(qx)
    const now = new Date('2026-10-28T12:00:00.000Z')
    await upsertCredlyBadges(qx, [
      badge('e-due', target, { expiresAt: '2026-10-01T00:00:00.000Z' }),
      badge('e-future', target, { expiresAt: '2027-10-01T00:00:00.000Z' }),
      badge('e-revoked', target, { state: 'revoked', expiresAt: '2026-10-01T00:00:00.000Z' }),
      badge('e-none', target),
      badge('e-earlier', target, { expiresAt: '2026-09-01T00:00:00.000Z' }),
    ])

    const due = await listDueCredlyExpiries(qx, now, 10)
    expect(due.map((row) => row.badgeId)).toEqual(['e-earlier', 'e-due'])

    expect(await markCredlyExpiriesEmitted(qx, ['e-earlier', 'e-due'])).toBe(2)
    expect(await markCredlyExpiriesEmitted(qx, ['e-earlier'])).toBe(0)
    expect(await listDueCredlyExpiries(qx, now, 10)).toEqual([])
  })

  test('a later extension of the expiry date does not re-emit an expired badge', async ({ qx }) => {
    const target = await project(qx)
    await upsertCredlyBadges(qx, [
      badge('e-ext', target, { expiresAt: '2026-10-01T00:00:00.000Z' }),
    ])
    await markCredlyExpiriesEmitted(qx, ['e-ext'])

    await upsertCredlyBadges(qx, [
      badge('e-ext', target, { expiresAt: '2026-10-20T00:00:00.000Z' }),
    ])

    expect(await listDueCredlyExpiries(qx, new Date('2026-10-28T00:00:00.000Z'), 10)).toEqual([])
  })
})
