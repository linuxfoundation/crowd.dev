import { describe, expect, it, vi } from 'vitest'

import type { TinybirdClient } from '@crowd/database'

import { readGithubChannelActivity } from './githubChannelActivity'

describe('readGithubChannelActivity', () => {
  it('keys 30-day activity by lower-cased channel', async () => {
    const executeSql = vi.fn().mockResolvedValue({
      data: [{ channelKey: 'https://github.com/a/b', activities: 12 }],
    })

    const result = await readGithubChannelActivity(
      { executeSql } as unknown as TinybirdClient,
      new Date('2026-10-22T03:00:00Z'),
    )

    expect(result.get('https://github.com/a/b')).toBe(12)
    const [query, params] = executeSql.mock.calls[0]
    expect(query).toContain('uniqExact(activityId)')
    expect(query).toContain("platform = 'github'")
    expect(params.since).toBe('2026-09-22 03:00:00.000')
  })
})
