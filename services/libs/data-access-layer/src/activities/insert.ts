import { pick } from 'lodash'
import moment from 'moment'

import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'
import { ACTIVITIES_QUEUE_SETTINGS, IQueue, QueueEmitter } from '@crowd/queue'
import telemetry from '@crowd/telemetry'

import { IDbActivityCreateData } from '../old/apps/data_sink_worker/repo/activity.data'
import { ACTIVITY_ALL_COLUMNS } from './sql'

const logger = getServiceChildLogger('insert-activities')

function isTrue(value: unknown): boolean {
  if (value === null || value === undefined) {
    return false
  }

  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'string') {
    return value.toLowerCase() === 'true'
  }

  return false
}

function toInt(value: unknown): number {
  if (value === null || value === undefined) {
    return 0
  }

  if (typeof value === 'number') {
    return value
  }

  if (typeof value === 'string') {
    const num = parseInt(value, 10)
    return isNaN(num) ? 0 : num
  }

  return 0
}

function truncateWithDots(s: string, max: number): string {
  if (!s || s.length <= max) {
    return s
  }

  // Cut by UTF-16 code units (fast, but may split surrogate pairs)
  let out = s.slice(0, max)

  // If the last code unit is a HIGH surrogate (D800–DBFF), drop it to avoid a dangling pair
  if (/[\uD800-\uDBFF]$/.test(out)) {
    out = out.slice(0, -1)
  }

  // Remove any trailing combining marks (prevents leaving accents/modifiers without a base)
  out = out.replace(/\p{M}+$/u, '')

  return out + '...'
}

export async function insertActivities(
  queueClient: IQueue,
  activities: IDbActivityCreateData[],
): Promise<string[]> {
  const now = moment().toISOString()

  const toInsert = activities
    .map((activity) => {
      const id = activity.id || generateUUIDv4()
      const attributes = serializeAttributes(tryToUnwrapAttributes(activity.attributes))
      if (attributes.pruned) {
        logger.warn(
          {
            activityId: id,
            platform: activity.platform,
            type: activity.type,
            sourceId: activity.sourceId,
            originalBytes: attributes.originalBytes,
            maxBytes: ATTRIBUTES_MAX_BYTES,
          },
          'Activity attributes exceeded the size cap and were pruned!',
        )
      }

      return {
        // we keep these ones in front of `...activity` because these fields might exist in the activity object
        member_isBot: activity.isBotActivity || false,
        member_isTeamMember: activity.isTeamMemberActivity || false,
        gitIsMainBranch: isTrue(activity.attributes['isMainBranch']),
        gitIsIndirectFork: isTrue(activity.attributes['isIndirectFork']),
        gitInsertions: activity.attributes['insertions']
          ? toInt(activity.attributes['insertions'])
          : toInt(activity.attributes['additions']),
        gitDeletions: toInt(activity.attributes['deletions']),
        gitLines: toInt(activity.attributes['lines']),
        gitIsMerge: isTrue(activity.attributes['isMerge']),

        ...activity,

        id,
        updatedAt: now,
        createdAt: activity.createdAt ? moment(activity.createdAt).toISOString() : now,
        timestamp: activity.timestamp ? moment(activity.timestamp).toISOString() : now,
        attributes: attributes.json,
        body: truncateWithDots(activity.body, 2000),

        // This is being added here temporarily, just to ensure we keep sending isContribution to Kafka
        // (and thus, Tinybird) after removing it from everywhere else, but this is meant to go away after we remove the
        // actual column.
        isContribution: false,
      }
    })
    .map((activity) => {
      return {
        // isContribution is added here because it was removed from ACTIVITY_ALL_COLUMNS but just as described a few
        // lines up, we still want to send it to Kafka for now. We should remove this once we remove the column from
        // Tinybird.
        ...pick(activity, [...ACTIVITY_ALL_COLUMNS, 'isContribution']),
        tenantId: DEFAULT_TENANT_ID,
      }
    })

  const emitter = new QueueEmitter(queueClient, ACTIVITIES_QUEUE_SETTINGS, logger)

  const commitTimestamp = new Date().toISOString()
  const bulk = toInsert.map((row) => {
    logger.debug(
      {
        activityId: row.id,
        queue: ACTIVITIES_QUEUE_SETTINGS.name,
      },
      'Dispatching activity to queue!',
    )
    return {
      payload: { ...row, metadata: { commit_timestamp: commitTimestamp } },
      groupId: generateUUIDv4(),
      deduplicationId: generateUUIDv4(),
    }
  })

  await emitter.sendMessages(bulk)
  telemetry.increment('tinybird.insert_activity', activities.length)

  return toInsert.map((activity) => activity.id)
}

const ATTRIBUTE_STRING_MAX_LENGTH = 500

// keeps a 10-message Kafka batch far below the 1 MB broker default
const ATTRIBUTES_MAX_BYTES = 16 * 1024

export function serializeAttributes(input: object): {
  json: string
  pruned: boolean
  originalBytes: number
} {
  const trimmed = trimAttributeStrings(input ?? {}) as object
  const json = JSON.stringify(trimmed)
  const originalBytes = Buffer.byteLength(json)
  if (originalBytes <= ATTRIBUTES_MAX_BYTES) {
    return { json, pruned: false, originalBytes }
  }

  return {
    json: JSON.stringify(pruneToFit(trimmed, ATTRIBUTES_MAX_BYTES)),
    pruned: true,
    originalBytes,
  }
}

export function trimAttributeStrings(value: unknown): unknown {
  if (typeof value === 'string') {
    return truncateWithDots(value, ATTRIBUTE_STRING_MAX_LENGTH)
  }
  if (Array.isArray(value)) {
    return value.map(trimAttributeStrings)
  }
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, trimAttributeStrings(v)]))
  }
  return value
}

// Largest entries go first (ties: object keys alphabetically, array items from the end); containers shrink from the
// inside, anything else is dropped.
function pruneToFit(value: object, maxBytes: number): object {
  const isArray = Array.isArray(value)
  const entries = Object.entries(value).map(([key, v]) => {
    const valueJson = JSON.stringify(v)
    const valueBytes = valueJson === undefined ? (isArray ? 4 : 0) : Buffer.byteLength(valueJson)
    const size =
      isArray || valueJson === undefined
        ? valueBytes
        : Buffer.byteLength(JSON.stringify(key)) + 1 + valueBytes
    return { key, value: v as unknown, valueBytes, size, keep: true }
  })

  const present = entries.filter((e) => e.size > 0)
  let total = 2 + present.reduce((sum, e) => sum + e.size, 0) + Math.max(present.length - 1, 0)
  let count = present.length

  const tieBreak = isArray
    ? (a: { key: string }, b: { key: string }) => Number(b.key) - Number(a.key)
    : (a: { key: string }, b: { key: string }) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)
  const bySizeDesc = [...present].sort((a, b) => b.size - a.size || tieBreak(a, b))

  for (const entry of bySizeDesc) {
    if (total <= maxBytes) {
      break
    }

    const excess = total - maxBytes
    if (entry.value && typeof entry.value === 'object' && entry.valueBytes - excess >= 2) {
      entry.value = pruneToFit(entry.value, entry.valueBytes - excess)
      const newBytes = Buffer.byteLength(JSON.stringify(entry.value))
      total -= entry.valueBytes - newBytes
      entry.valueBytes = newBytes
      continue
    }

    entry.keep = false
    total -= entry.size + (count > 1 ? 1 : 0)
    count -= 1
  }

  const kept = entries.filter((e) => e.keep)
  return isArray ? kept.map((e) => e.value) : Object.fromEntries(kept.map((e) => [e.key, e.value]))
}

export function tryToUnwrapAttributes(attributes: unknown): object {
  let value = attributes
  while (typeof value === 'string') {
    value = JSON.parse(value)
  }
  return value !== null && typeof value === 'object' ? value : {}
}
