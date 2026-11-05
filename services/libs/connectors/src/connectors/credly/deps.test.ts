import { describe, expect, it, vi } from 'vitest'

import {
  findCredlyBadges,
  listDueCredlyExpiries,
  markCredlyExpiriesEmitted,
  upsertCredlyBadges,
} from '@crowd/data-access-layer/src/credly/badges'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { createCredlyDeps } from './deps'

vi.mock('@crowd/data-access-layer/src/credly/badges', () => ({
  findCredlyBadges: vi.fn(),
  upsertCredlyBadges: vi.fn(),
  listDueCredlyExpiries: vi.fn(),
  markCredlyExpiriesEmitted: vi.fn(),
}))

const qx = {} as QueryExecutor

describe('createCredlyDeps', () => {
  it('throws when the integration has no segment', () => {
    expect(() => createCredlyDeps({ qx, integrationId: 'anchor', segmentId: null })).toThrow(
      'credly integration anchor has no segment',
    )
  })

  it('exposes the integration segment', () => {
    const deps = createCredlyDeps({ qx, integrationId: 'int-k8s', segmentId: 'k8s' })
    expect(deps.segmentId).toBe('k8s')
  })

  it('reads and writes the badge state through the table', async () => {
    vi.mocked(findCredlyBadges).mockResolvedValueOnce([])
    const deps = createCredlyDeps({ qx, integrationId: 'int-k8s', segmentId: 'k8s' })

    expect(await deps.findBadges(['b-1'])).toEqual([])
    await deps.saveBadges([])

    expect(findCredlyBadges).toHaveBeenCalledWith(qx, ['b-1'])
    expect(upsertCredlyBadges).toHaveBeenCalledWith(qx, [])
  })

  it('reads and marks due expiries through the table', async () => {
    vi.mocked(listDueCredlyExpiries).mockResolvedValueOnce([])
    vi.mocked(markCredlyExpiriesEmitted).mockResolvedValueOnce(2)
    const deps = createCredlyDeps({ qx, integrationId: 'int-k8s', segmentId: 'k8s' })
    const now = new Date('2026-10-28T12:00:00.000Z')

    expect(await deps.listDueExpiries(now, 10)).toEqual([])
    expect(await deps.markExpiriesEmitted(['a', 'b'])).toBe(2)

    expect(listDueCredlyExpiries).toHaveBeenCalledWith(qx, now, 10, undefined)
    expect(markCredlyExpiriesEmitted).toHaveBeenCalledWith(qx, ['a', 'b'])
  })
})
