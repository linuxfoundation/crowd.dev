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
        attributes: objectToBytes(tryToUnwrapAttributes(activity.attributes)),
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

  for (const row of toInsert) {
    logger.debug(
      {
        activityId: row.id,
        queue: ACTIVITIES_QUEUE_SETTINGS.name,
      },
      'Dispatching activity to queue!',
    )
    const messageWithMetadata = {
      ...row,
      metadata: {
        commit_timestamp: new Date().toISOString(),
      },
    }
    logger.debug(
      {
        activityId: row.id,
        commitTimestamp: messageWithMetadata.metadata.commit_timestamp,
      },
      'Sending activity with metadata to Kafka',
    )
    await emitter.sendMessage(generateUUIDv4(), messageWithMetadata, generateUUIDv4())
  }
  telemetry.increment('tinybird.insert_activity', activities.length)

  return toInsert.map((activity) => activity.id)
}

function objectToBytes(input: object): string {
  if (typeof input !== 'object') {
    return input
  }

  if (!input) {
    input = {}
  }

  const stringified = JSON.stringify(input)

  // check size
  if (new TextEncoder().encode(stringified).length > 2000) {
    return JSON.stringify({})
  }

  return JSON.stringify(input)
}

function tryToUnwrapAttributes(attributes: string | object): object {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (typeof attributes === 'object') {
      return attributes
    }
    attributes = JSON.parse(attributes)
  }
}
