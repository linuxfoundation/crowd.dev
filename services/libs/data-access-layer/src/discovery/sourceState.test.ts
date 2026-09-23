import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  findDiscoverySourceCursor,
  findDiscoverySourceWatermark,
  upsertDiscoverySourceCursor,
  upsertDiscoverySourceWatermark,
} from './sourceState'

const test = withQx(base)

describe('findDiscoverySourceWatermark', () => {
  test('returns null when the source has no stored watermark', async ({ qx }) => {
    expect(await findDiscoverySourceWatermark(qx, 'insights-discussions')).toBeNull()
  })
})

describe('upsertDiscoverySourceWatermark', () => {
  test('inserts the watermark for a new source', async ({ qx }) => {
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-01T00:00:00.000Z')

    const watermark = await findDiscoverySourceWatermark(qx, 'insights-discussions')
    expect(new Date(watermark as string).toISOString()).toBe('2026-01-01T00:00:00.000Z')
  })

  test('advances the watermark forward', async ({ qx }) => {
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-01T00:00:00.000Z')
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-02T00:00:00.000Z')

    const watermark = await findDiscoverySourceWatermark(qx, 'insights-discussions')
    expect(new Date(watermark as string).toISOString()).toBe('2026-01-02T00:00:00.000Z')
  })

  test('rejects moving the watermark backwards without force', async ({ qx }) => {
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-02T00:00:00.000Z')
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-01T00:00:00.000Z')

    const watermark = await findDiscoverySourceWatermark(qx, 'insights-discussions')
    expect(new Date(watermark as string).toISOString()).toBe('2026-01-02T00:00:00.000Z')
  })

  test('moves the watermark backwards when force is true', async ({ qx }) => {
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-02T00:00:00.000Z')
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-01T00:00:00.000Z', {
      force: true,
    })

    const watermark = await findDiscoverySourceWatermark(qx, 'insights-discussions')
    expect(new Date(watermark as string).toISOString()).toBe('2026-01-01T00:00:00.000Z')
  })

  test('keeps watermarks isolated per source', async ({ qx }) => {
    await upsertDiscoverySourceWatermark(qx, 'insights-discussions', '2026-01-01T00:00:00.000Z')
    await upsertDiscoverySourceWatermark(qx, 'lf-criticality-score', '2026-02-01T00:00:00.000Z')

    expect(
      new Date(
        (await findDiscoverySourceWatermark(qx, 'insights-discussions')) as string,
      ).toISOString(),
    ).toBe('2026-01-01T00:00:00.000Z')
    expect(
      new Date(
        (await findDiscoverySourceWatermark(qx, 'lf-criticality-score')) as string,
      ).toISOString(),
    ).toBe('2026-02-01T00:00:00.000Z')
  })
})

describe('findDiscoverySourceCursor', () => {
  test('returns null when the source has no stored cursor', async ({ qx }) => {
    expect(await findDiscoverySourceCursor(qx, 'lf-criticality-score')).toBeNull()
  })
})

describe('upsertDiscoverySourceCursor', () => {
  test('inserts the cursor for a new source', async ({ qx }) => {
    await upsertDiscoverySourceCursor(qx, 'lf-criticality-score', {
      rundate: '2026-09-01',
      page: 3,
    })

    expect(await findDiscoverySourceCursor(qx, 'lf-criticality-score')).toEqual({
      rundate: '2026-09-01',
      page: 3,
    })
  })

  test('replaces the cursor outright, including moving the page backwards', async ({ qx }) => {
    await upsertDiscoverySourceCursor(qx, 'lf-criticality-score', {
      rundate: '2026-09-01',
      page: 37,
    })
    await upsertDiscoverySourceCursor(qx, 'lf-criticality-score', { rundate: '2026-10-01', page: 0 })

    expect(await findDiscoverySourceCursor(qx, 'lf-criticality-score')).toEqual({
      rundate: '2026-10-01',
      page: 0,
    })
  })

  test('keeps cursors isolated per source', async ({ qx }) => {
    await upsertDiscoverySourceCursor(qx, 'lf-criticality-score', {
      rundate: '2026-09-01',
      page: 3,
    })
    await upsertDiscoverySourceCursor(qx, 'insights-discussions', {
      rundate: '2026-09-02',
      page: 1,
    })

    expect(await findDiscoverySourceCursor(qx, 'lf-criticality-score')).toEqual({
      rundate: '2026-09-01',
      page: 3,
    })
    expect(await findDiscoverySourceCursor(qx, 'insights-discussions')).toEqual({
      rundate: '2026-09-02',
      page: 1,
    })
  })
})
