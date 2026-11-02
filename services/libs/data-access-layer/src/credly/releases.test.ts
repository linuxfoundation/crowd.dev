import { test as base, describe, expect } from 'vitest'

import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  completeCredlyTemplateRelease,
  nextPendingCredlyTemplateRelease,
  queueCredlyTemplateRelease,
  saveCredlyTemplateReleaseCursor,
} from './releases'

const test = withQx(base)

const FLOOR = '2022-10-25T00:00:00.000Z'
const TOP = '2026-11-02T10:00:00.000Z'

async function emptyQueue(qx: QueryExecutor): Promise<void> {
  await qx.result(`DELETE FROM integration."credlyTemplateReleases"`)
}

async function queue(qx: QueryExecutor, badgeTemplateId: string, floor = FLOOR, top = TOP) {
  await emptyQueue(qx)
  await queueCredlyTemplateRelease(qx, { badgeTemplateId, floor, top })
  const release = await nextPendingCredlyTemplateRelease(qx)
  if (release?.badgeTemplateId !== badgeTemplateId) throw new Error('queue is not empty')
  return release
}

describe('credly template releases', () => {
  test('serves pending releases in queue order and skips completed ones', async ({ qx }) => {
    const first = await queue(qx, 'tpl-first')
    await queueCredlyTemplateRelease(qx, { badgeTemplateId: 'tpl-second', floor: FLOOR, top: TOP })

    expect((await nextPendingCredlyTemplateRelease(qx))?.badgeTemplateId).toBe('tpl-first')
    expect(await completeCredlyTemplateRelease(qx, first)).toBe(1)
    const second = await nextPendingCredlyTemplateRelease(qx)
    expect(second?.badgeTemplateId).toBe('tpl-second')
    expect(await completeCredlyTemplateRelease(qx, second)).toBe(1)
    expect(await nextPendingCredlyTemplateRelease(qx)).toBeNull()
    expect(await completeCredlyTemplateRelease(qx, second)).toBe(0)
  })

  test('keeps the window cursor of a release in progress', async ({ qx }) => {
    const release = await queue(qx, 'tpl')

    expect(
      await saveCredlyTemplateReleaseCursor(qx, release, {
        top: '2026-10-26T10:00:00.000Z',
        since: '2026-10-20T00:00:00.000Z',
        until: '2026-10-26T10:00:00.000Z',
        page: 3,
      }),
    ).toBe(1)

    const stored = await nextPendingCredlyTemplateRelease(qx)
    expect(stored).toMatchObject({ badgeTemplateId: 'tpl', page: 3 })
    expect(new Date(stored.top).toISOString()).toBe('2026-10-26T10:00:00.000Z')
    expect(new Date(stored.since).toISOString()).toBe('2026-10-20T00:00:00.000Z')
    expect(new Date(stored.until).toISOString()).toBe('2026-10-26T10:00:00.000Z')
  })

  test('queuing a template again restarts it, and the walk in flight loses its writes', async ({
    qx,
  }) => {
    const stale = await queue(qx, 'tpl')
    await completeCredlyTemplateRelease(qx, stale)

    await queueCredlyTemplateRelease(qx, {
      badgeTemplateId: 'tpl',
      floor: '2024-01-01T00:00:00.000Z',
      top: '2026-11-03T10:00:00.000Z',
    })

    const fresh = await nextPendingCredlyTemplateRelease(qx)
    expect(fresh).toMatchObject({ badgeTemplateId: 'tpl', page: 1, since: null, until: null })
    expect(new Date(fresh.floor).toISOString()).toBe('2024-01-01T00:00:00.000Z')
    expect(fresh.completedAt).toBeNull()
    expect(
      await saveCredlyTemplateReleaseCursor(qx, stale, {
        top: TOP,
        since: null,
        until: null,
        page: 2,
      }),
    ).toBe(0)
    expect(await completeCredlyTemplateRelease(qx, stale)).toBe(0)
  })

  test('rejects a floor that does not sit below the top', async ({ qx }) => {
    await emptyQueue(qx)
    await expect(
      queueCredlyTemplateRelease(qx, { badgeTemplateId: 'tpl', floor: TOP, top: FLOOR }),
    ).rejects.toThrow()
  })
})
