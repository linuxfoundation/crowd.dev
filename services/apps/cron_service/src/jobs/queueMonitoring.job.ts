import CronTime from 'cron-time-generator'

import { IS_PROD_ENV, distinct, timeout } from '@crowd/common'
import { Logger } from '@crowd/logging'
import { KafkaAdmin, QUEUE_CONFIG, getKafkaClient, getKafkaMessageCounts } from '@crowd/queue'
import { REDIS_CONFIG, RedisClient, getRedisClient } from '@crowd/redis'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'
import telemetry from '@crowd/telemetry'

import { IJobDefinition } from '../types'

const STALL_CHECKED_TOPIC_PREFIX = 'data-sink-worker-'
const STALL_SNAPSHOT_KEY_PREFIX = 'queue-monitoring:partitions'
const STALL_SNAPSHOT_TTL_SECONDS = 2 * 60 * 60
const REDIS_OPERATION_TIMEOUT_MS = 5000

interface IPartitionSnapshot {
  committed: number
  lag: number
  stalledSince?: number
}

interface IGroupSnapshot {
  at: number
  partitions: Record<string, IPartitionSnapshot>
}

const job: IJobDefinition = {
  name: 'queue-monitoring',
  cronTime: CronTime.every(30).minutes(),
  timeout: 30 * 60,
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    const toIgnore = (
      process.env.CROWD_KAFKA_STREAMS_TO_IGNORE_ON_MONITORING?.split(',') ?? []
    ).map((s) => s.trim())

    ctx.log.info(`To ignore: ${toIgnore.join(', ')}`)

    const kafkaClient = getKafkaClient(QUEUE_CONFIG())
    const admin = kafkaClient.admin()
    await admin.connect()

    const map = await getTopicsAndConsumerGroups(ctx.log, admin)
    const redis = await connectRedis(ctx.log)

    let msg = ``

    for (const [topic, groups] of map) {
      const totalMessages = await getTopicMessageCount(ctx.log, admin, topic)
      telemetry.gauge(`kafka.${topic}.total`, totalMessages)

      if (groups.length === 0) {
        if (toIgnore.includes(topic.trim())) {
          ctx.log.info(`Ignoring topic ${topic}!`)
        } else {
          msg += `No consumer groups found for topic ${topic}! Total messages in topic: ${totalMessages}\n`
        }
      }

      for (const group of groups) {
        const counts = await getKafkaMessageCounts(ctx.log, admin, topic, group)
        ctx.log.info(
          `Topic ${topic} group ${group} has ${counts.total} total messages, ${counts.consumed} consumed, ${counts.unconsumed} unconsumed!`,
        )

        telemetry.gauge(`kafka.${topic}.${group}.consumed`, counts.consumed)
        telemetry.gauge(`kafka.${topic}.${group}.unconsumed`, counts.unconsumed)
      }

      if (topic.startsWith(STALL_CHECKED_TOPIC_PREFIX) && !toIgnore.includes(topic.trim())) {
        if (!redis) {
          msg += `Stall check for topic ${topic} skipped: Redis is unavailable!\n`
        } else if (groups.includes(topic)) {
          const stalled = await findStalledPartitions(ctx.log, admin, redis, topic)
          if (stalled) {
            msg += `${stalled}\n`
          }
        }
      }
    }

    if (msg && msg.trim().length > 0) {
      await sendSlackNotificationAsync(
        SlackChannel.CDP_CRITICAL_ALERTS,
        SlackPersona.WARNING_PROPAGATOR,
        'Queue Monitoring Alert',
        msg,
      )
      ctx.log.info('Queue monitoring alert sent to Slack')
    }

    telemetry.flush()

    // sleep for 30 seconds to properly flush the metrics
    await timeout(30000)
  },
}

async function connectRedis(log: Logger): Promise<RedisClient | undefined> {
  try {
    // without a listener a dropped connection is an unhandled 'error' event that kills the job
    return await withTimeout(
      getRedisClient(REDIS_CONFIG(), false, (err) =>
        log.warn(err, 'Redis client error during queue monitoring!'),
      ),
    )
  } catch (err) {
    log.error(err, 'Failed to connect to Redis, skipping the consumer stall check!')
    return undefined
  }
}

async function withTimeout<T>(promise: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('Redis operation timed out!')),
          REDIS_OPERATION_TIMEOUT_MS,
        )
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

async function readPartitionOffsets(
  admin: KafkaAdmin,
  topic: string,
): Promise<Record<string, { committed: number; lag: number }>> {
  const topicOffsets = await admin.fetchTopicOffsets(topic)
  const groupOffsets = await admin.fetchOffsets({ groupId: topic, topics: [topic] })

  const result: Record<string, { committed: number; lag: number }> = {}
  for (const { partition, offset } of groupOffsets[0]?.partitions ?? []) {
    const topicOffset = topicOffsets.find((o) => o.partition === partition)
    if (!topicOffset || offset === '-1') {
      continue
    }

    const high = Number(topicOffset.offset)
    const committed = Number(offset)
    result[partition] = {
      committed,
      lag: Math.max(0, high - Math.max(committed, Number(topicOffset.low))),
    }
  }

  return result
}

function formatDuration(ms: number): string {
  const minutes = Math.round(ms / 60000)
  return minutes < 60 ? `${minutes} min` : `${Math.floor(minutes / 60)} h ${minutes % 60} min`
}

async function findStalledPartitions(
  log: Logger,
  admin: KafkaAdmin,
  redis: RedisClient,
  topic: string,
): Promise<string | undefined> {
  try {
    const key = `${STALL_SNAPSHOT_KEY_PREFIX}:${topic}`
    const previousValue = await withTimeout(redis.get(key))
    const previous: IGroupSnapshot | undefined = previousValue
      ? JSON.parse(previousValue)
      : undefined

    const now = Date.now()
    const current: IGroupSnapshot = { at: now, partitions: {} }
    let stalledLag = 0
    let stalledSince = now
    const stalledPartitions: string[] = []

    for (const [partition, offsets] of Object.entries(await readPartitionOffsets(admin, topic))) {
      const before = previous?.partitions[partition]
      const stalled =
        before !== undefined &&
        before.lag > 0 &&
        offsets.lag > 0 &&
        before.committed === offsets.committed

      const snapshot: IPartitionSnapshot = { ...offsets }
      if (stalled) {
        snapshot.stalledSince = before.stalledSince ?? previous.at
        stalledPartitions.push(partition)
        stalledLag += offsets.lag
        stalledSince = Math.min(stalledSince, snapshot.stalledSince)
      }
      current.partitions[partition] = snapshot
    }

    await withTimeout(redis.set(key, JSON.stringify(current), { EX: STALL_SNAPSHOT_TTL_SECONDS }))

    if (stalledPartitions.length === 0) {
      return undefined
    }

    log.warn({ topic, stalledPartitions, stalledLag }, 'Consumer group stopped consuming!')
    const total = Object.keys(current.partitions).length
    return (
      `Consumer group ${topic}: ${stalledPartitions.length} of ${total} partitions consumed ` +
      `nothing since ${new Date(stalledSince).toISOString()} (${formatDuration(now - stalledSince)}) ` +
      `while ${stalledLag.toLocaleString('en-US')} messages wait on them!`
    )
  } catch (err) {
    log.error(err, `Failed to check topic ${topic} for stalled partitions!`)
    return `Stall check for topic ${topic} failed: ${err.message}`
  }
}

async function getTopicsAndConsumerGroups(
  log: Logger,
  admin: KafkaAdmin,
): Promise<Map<string, string[]>> {
  const topics = await admin.listTopics()

  const topicConsumerMap = new Map<string, string[]>()

  const groupsResponse = await admin.listGroups()
  const consumerGroups = distinct(groupsResponse.groups.map((g) => g.groupId))

  for (const topic of topics) {
    if (topicConsumerMap.has(topic)) {
      continue
    }

    log.debug(`Checking topic ${topic} consumer groups!`)
    const consumers = []
    for (const group of consumerGroups) {
      log.debug(`Checking group ${group} if it is listening to topic ${topic}!`)
      if (await isConsumerListeningToTopic(log, admin, group, topic)) {
        log.debug(`Group ${group} is listening to topic ${topic}!`)
        consumers.push(group)
      }
    }
    topicConsumerMap.set(topic, consumers)
  }

  return topicConsumerMap
}

async function isConsumerListeningToTopic(
  log: Logger,
  admin: KafkaAdmin,
  groupId: string,
  topic: string,
): Promise<boolean> {
  try {
    // Method 1: Check subscriptions via group description (most accurate)
    try {
      const groupDetails = await admin.describeGroups([groupId])

      // Skip empty or Dead groups
      if (!groupDetails.groups[0] || groupDetails.groups[0].state === 'Dead') {
        log.debug(`Group ${groupId} is dead!`)
        return false
      }

      // Check if any member is subscribed to the topic
      for (const member of groupDetails.groups[0].members) {
        if (member.memberMetadata) {
          try {
            // Parse member metadata which contains subscription info
            const metadata = JSON.parse(member.memberMetadata.toString())

            // Check if topic is in the subscription list
            if (metadata.subscription && Array.isArray(metadata.subscription)) {
              if (metadata.subscription.includes(topic)) {
                log.debug(`Group ${groupId} is listening to topic ${topic}!`)
                return true
              }
            }
          } catch {
            // log.error(
            //   metadataErr,
            //   `Failed to parse metadata for group ${groupId}! - "${member.memberMetadata.toString()}"`,
            // )
            // Failed to parse metadata, try alternative approach
          }
        }
      }
    } catch (describeErr) {
      // Group might not exist or other error occurred
      // Fall through to alternative method
      log.error(describeErr, `Error describing group ${groupId}`)
    }

    // Method 2: Check committed offsets (less accurate but works as fallback)
    try {
      const offsetsResponse = await admin.fetchOffsets({
        groupId: groupId,
        topics: [topic],
      })

      // No data returned
      if (offsetsResponse.length === 0 || !offsetsResponse[0].partitions) {
        log.debug(`Group ${groupId} is not listening to topic ${topic}!`)
        return false
      }

      // Check if any partition has a valid offset
      const hasConsumedPartitions = offsetsResponse[0].partitions.some((partition) => {
        // An offset of -1 means the consumer has never committed an offset
        // Any other value means it has consumed from this topic
        log.debug(
          `Group ${groupId} Partition ${partition.partition} has offset ${partition.offset}!`,
        )
        return partition.offset !== '-1'
      })

      return hasConsumedPartitions
    } catch (offsetErr) {
      // Likely means this consumer doesn't consume this topic
      log.error(offsetErr, `Group ${groupId} is not listening to topic ${topic}!`)
      return false
    }
  } catch (error) {
    // Log the error but don't crash the application
    log.error(error, `Error checking if group ${groupId} listens to topic ${topic}`)
    return false
  }
}

async function getTopicMessageCount(
  log: Logger,
  admin: KafkaAdmin,
  topic: string,
): Promise<number> {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(topic)

    // Sum (high - low) per partition — actual messages currently in the topic.
    const totalMessages = topicOffsets.reduce((sum, partition) => {
      return sum + Math.max(0, Number(partition.offset) - Number(partition.low))
    }, 0)

    return totalMessages
  } catch (err) {
    log.error(err, `Failed to get message count for topic ${topic}!`)
    throw err
  }
}

export default job
