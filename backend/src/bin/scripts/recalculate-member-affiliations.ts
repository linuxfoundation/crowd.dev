import commandLineArgs from 'command-line-args'
import validator from 'validator'

import { getServiceLogger } from '@crowd/logging'
import { getRedisClient } from '@crowd/redis'

import { REDIS_CONFIG } from '@/conf'

const RECALCULATE_MEMBER_AFFILIATIONS_KEY = 'recalculate-member-affiliations'

const log = getServiceLogger()

const options = [
  {
    name: 'memberIds',
    alias: 'm',
    type: String,
    description: 'Comma-separated member IDs to queue for affiliation recalculation.',
  },
]

const parameters = commandLineArgs(options) as {
  memberIds?: string
}

setImmediate(async () => {
  if (!parameters.memberIds) {
    throw new Error('memberIds is required')
  }

  const memberIds: string[] = Array.from(
    new Set(
      parameters.memberIds
        .split(',')
        .map((id: string) => id.trim())
        .filter(Boolean),
    ),
  )

  const invalidMemberIds = memberIds.filter((id) => !validator.isUUID(id))
  if (invalidMemberIds.length > 0) {
    throw new Error(`Invalid member IDs: ${invalidMemberIds.join(', ')}`)
  }

  if (memberIds.length === 0) {
    log.info('No member IDs provided!')
    return
  }

  const redis = await getRedisClient(REDIS_CONFIG, true)

  await redis.sAdd(RECALCULATE_MEMBER_AFFILIATIONS_KEY, memberIds)

  log.info(
    {
      count: memberIds.length,
      memberIds,
      redisKey: RECALCULATE_MEMBER_AFFILIATIONS_KEY,
    },
    'Queued members for affiliation recalculation!',
  )
})
