import type { RedisClient } from './types'

export const NOT_MARKED = 0

// Readers take now() before they read; writers call markChanged after their write commits, so a
// change a reader may have missed is always marked later than the clock value the reader took
export interface IChangeClock {
  now(): Promise<number>
  markChanged(ids: string[]): Promise<{ at: number; previous: number[] }>
  // All keys share one cluster slot, for callers that read them in their own scripts
  clockKey(): string
  changedKeyPrefix(): string
}

const MARK_CHANGED = `
local at = redis.call('incr', KEYS[1])
local previous = {}
for i = 2, #KEYS do
  previous[i - 1] = tonumber(redis.call('get', KEYS[i])) or 0
  redis.call('set', KEYS[i], at, 'EX', ARGV[1])
end
return {at, previous}`

export function createChangeClock(
  client: RedisClient,
  name: string,
  retentionSeconds: number,
): IChangeClock {
  const prefix = `change-clock:{${name}}`
  const clockKey = () => `${prefix}:clock`
  const changedKeyPrefix = () => `${prefix}:changed:`

  return {
    clockKey,
    changedKeyPrefix,

    now: async () => Number((await client.get(clockKey())) ?? NOT_MARKED),

    markChanged: async (ids) => {
      const [at, previous] = (await client.eval(MARK_CHANGED, {
        keys: [clockKey(), ...ids.map((id) => `${changedKeyPrefix()}${id}`)],
        arguments: [String(retentionSeconds)],
      })) as [number, number[]]
      return { at: Number(at), previous: previous.map(Number) }
    },
  }
}
