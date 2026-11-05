import type { TinybirdClient } from '@crowd/database'

const ACTIVITY_WINDOW_DAYS = 30
const CREATED_AT_BOUND_DAYS = 400
const DAY_MS = 24 * 60 * 60 * 1000

const QUERY = `%
SELECT lower(channel) AS channelKey, uniqExact(activityId) AS activities
FROM activityRelations
WHERE platform = 'github'
  AND channel != ''
  AND timestamp >= parseDateTime64BestEffort({{String(since)}})
  AND createdAt >= parseDateTime64BestEffort({{String(createdSince)}})
GROUP BY channelKey
FORMAT JSON
`

type ChannelActivityRow = { channelKey: string; activities: number }

export function formatTinybirdDate(date: Date): string {
  return date.toISOString().replace('T', ' ').replace('Z', '')
}

export async function readGithubChannelActivity(
  tb: TinybirdClient,
  now: Date,
): Promise<Map<string, number>> {
  const result = await tb.executeSql<{ data: ChannelActivityRow[] }>(QUERY, {
    since: formatTinybirdDate(new Date(now.getTime() - ACTIVITY_WINDOW_DAYS * DAY_MS)),
    createdSince: formatTinybirdDate(new Date(now.getTime() - CREATED_AT_BOUND_DAYS * DAY_MS)),
  })

  return new Map(result.data.map((row) => [row.channelKey, Number(row.activities)]))
}
