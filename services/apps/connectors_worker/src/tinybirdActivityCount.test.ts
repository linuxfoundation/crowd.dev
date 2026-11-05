import { describe, expect, it, vi } from 'vitest'

import type { CountWindow } from '@crowd/data-access-layer/src/connectors/types'
import type { TinybirdClient } from '@crowd/database'

import { countTinybirdActivities } from './tinybirdActivityCount'

function setup(data: { activities: string | number }[] = [{ activities: '7' }]) {
  const executeSql = vi.fn().mockResolvedValue({ data })
  const tb = { executeSql } as unknown as TinybirdClient
  const run = (segmentId: string | null, window: CountWindow) =>
    countTinybirdActivities(tb, {
      segmentId,
      platform: 'github',
      channel: 'https://github.com/a/b',
      activityTypes: ['issues-opened', 'pull_request-opened'],
      window,
    })
  return { executeSql, run }
}

describe('countTinybirdActivities', () => {
  it('sends formatted bounds for a range window', async () => {
    const { executeSql, run } = setup()

    await run('seg-1', {
      kind: 'range',
      from: '2026-10-01T00:00:00.000Z',
      to: '2026-10-02T00:00:00.000Z',
    })

    const [query, params] = executeSql.mock.calls[0]
    expect(query).toContain('uniqExact(activityId)')
    expect(query).toContain('timestamp <')
    expect(query).not.toMatch(/\{\{String\((from|to)\)\}\}/)
    expect(params.windowFrom).toBe('2026-10-01 00:00:00.000')
    expect(params.windowTo).toBe('2026-10-02 00:00:00.000')
    expect(params.segmentId).toBe('seg-1')
    expect(params.types).toEqual(['issues-opened', 'pull_request-opened'])

    await run('seg-1', { kind: 'all-time', to: '2026-10-02T00:00:00.000Z' })
    const [, boundedParams] = executeSql.mock.calls[1]
    expect(boundedParams.windowFrom).toBe('1970-01-01 00:00:00.000')
    expect(boundedParams.windowTo).toBe('2026-10-02 00:00:00.000')
  })

  it('sends no bounds for an all-time window and no segment when null', async () => {
    const { executeSql, run } = setup()

    const count = await run(null, { kind: 'all-time' })

    const [query, params] = executeSql.mock.calls[0]
    expect(count).toBe(7)
    expect(query).not.toContain('timestamp')
    expect(query).not.toContain('segmentId')
    expect(params).not.toHaveProperty('windowFrom')
    expect(params).not.toHaveProperty('windowTo')
    expect(params).not.toHaveProperty('segmentId')
  })

  it('throws on an empty result instead of returning 0', async () => {
    const { run } = setup([])

    await expect(run(null, { kind: 'all-time' })).rejects.toThrow()
  })
})
