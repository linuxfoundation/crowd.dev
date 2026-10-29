import type { CountWindow } from '@crowd/data-access-layer/src/connectors/types'
import type { TinybirdClient } from '@crowd/database'

import { formatTinybirdDate } from './githubChannelActivity'

type TinybirdActivityCountQuery = {
  segmentId: string | null
  platform: string
  channel: string
  activityTypes: string[]
  window: CountWindow
}

const SELECT_FRAGMENT = `%
SELECT uniqExact(activityId) AS activities
FROM activityRelations
WHERE platform = {{String(platform)}}
  AND lower(channel) = lower({{String(channel)}})
  AND type IN {{Array(types, 'String')}}`

const SEGMENT_FRAGMENT = `
  AND segmentId = {{String(segmentId)}}`

const RANGE_FRAGMENT = `
  AND timestamp >= parseDateTime64BestEffort({{String(windowFrom)}})
  AND timestamp < parseDateTime64BestEffort({{String(windowTo)}})`

const FORMAT_FRAGMENT = `
FORMAT JSON
`

export async function countTinybirdActivities(
  tb: TinybirdClient,
  query: TinybirdActivityCountQuery,
): Promise<number> {
  const { segmentId, platform, channel, activityTypes, window } = query

  const sql =
    SELECT_FRAGMENT +
    (segmentId === null ? '' : SEGMENT_FRAGMENT) +
    (window.kind === 'range' ? RANGE_FRAGMENT : '') +
    FORMAT_FRAGMENT

  const params: Record<string, unknown> = { platform, channel, types: activityTypes }
  if (segmentId !== null) {
    params.segmentId = segmentId
  }
  if (window.kind === 'range') {
    params.windowFrom = formatTinybirdDate(new Date(window.from))
    params.windowTo = formatTinybirdDate(new Date(window.to))
  }

  const result = await tb.executeSql<{ data: { activities: string | number }[] }>(sql, params)

  if (result.data.length === 0) {
    throw new Error('Tinybird activity count returned no rows')
  }

  return Number(result.data[0].activities)
}
