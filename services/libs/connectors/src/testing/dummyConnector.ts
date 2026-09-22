import { z } from 'zod'

import type { Manifest, SyncContext } from '../types'

const TICK_COUNT = 3

export const dummyConnector: Manifest = {
  platform: 'dummy',
  syncs: [
    {
      name: 'ticks',
      cadenceMinutes: 60,
      schema: z.record(z.unknown()),
      run: async (ctx: SyncContext) => {
        await ctx.emit(
          Array.from({ length: TICK_COUNT }, (_, index) => ({
            type: 'tick',
            sourceId: `tick-${index}`,
            timestamp: new Date().toISOString(),
            tick: index,
          })),
        )
        await ctx.commitWatermark({ since: new Date().toISOString() })
        return { complete: true }
      },
    },
  ],
  discover: async () => [{ channelId: 'dummy-channel', channelName: 'dummy/channel' }],
}
