import { beforeEach, describe, expect, it, vi } from 'vitest'

import { CHANNEL_CONCURRENCY, describeChannelError, shadowDiff } from './shadowDiff'

const mocks = vi.hoisted(() => ({
  listShadowDiffChannels: vi.fn(),
  runShadowDiffForChannel: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({
    listShadowDiffChannels: mocks.listShadowDiffChannels,
    runShadowDiffForChannel: mocks.runShadowDiffForChannel,
  }),
  log: mocks.log,
}))

describe('describeChannelError', () => {
  it('extracts the message from an Error so operators can see why a channel failed', () => {
    expect(describeChannelError(new Error('db exploded'))).toBe('db exploded')
  })

  it('stringifies non-Error throwables instead of losing the detail', () => {
    expect(describeChannelError('raw string failure')).toBe('raw string failure')
    expect(describeChannelError({ code: 'ECONNRESET' })).toBe('{"code":"ECONNRESET"}')
  })

  it('falls back to String() when the thrown value cannot be JSON-serialized', () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    expect(describeChannelError(circular)).toBe(String(circular))
  })
})

describe('shadowDiff', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('pages channels 50 at a time from the cursor and keeps counting after a failed channel', async () => {
    const channels = Array.from({ length: 120 }, (_, i) => ({
      channelName: `c${String(i).padStart(3, '0')}`,
      integrationId: 'i',
      units: [],
    }))
    mocks.listShadowDiffChannels.mockImplementation(async ({ after, limit }) => {
      const start = after
        ? channels.findIndex((channel) => channel.channelName === after.channelName) + 1
        : 0
      return channels.slice(start, start + limit)
    })
    let inFlight = 0
    let peak = 0
    mocks.runShadowDiffForChannel.mockImplementation(async (channel) => {
      inFlight += 1
      peak = Math.max(peak, inFlight)
      await Promise.resolve()
      inFlight -= 1
      if (channel.channelName === 'c007') throw new Error('boom')
      return { channelName: channel.channelName, integrationId: 'i', status: 'ok' }
    })
    await shadowDiff()
    expect(peak).toBe(CHANNEL_CONCURRENCY)
    expect(CHANNEL_CONCURRENCY).toBe(50)
    expect(mocks.listShadowDiffChannels.mock.calls.map(([input]) => input)).toEqual([
      { after: undefined, limit: 50 },
      { after: { channelName: 'c049', integrationId: 'i' }, limit: 50 },
      { after: { channelName: 'c099', integrationId: 'i' }, limit: 50 },
      { after: { channelName: 'c119', integrationId: 'i' }, limit: 50 },
    ])
    expect(mocks.log.info).toHaveBeenCalledWith(
      'shadow diff run complete',
      expect.objectContaining({ channelCount: 120, okCount: 119, errorCount: 1 }),
    )
  })
})
