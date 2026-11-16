import CronTime from 'cron-time-generator'

import { IS_PROD_ENV, distinct, timeout } from '@crowd/common'
import { Logger } from '@crowd/logging'
import {
  KafkaAdmin,
  KafkaAssignerProtocol,
  KafkaGroupDescription,
  QUEUE_CONFIG,
  getKafkaClient,
  getKafkaMessageCounts,
} from '@crowd/queue'
import { REDIS_CONFIG, RedisClient, getRedisClient } from '@crowd/redis'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'
import telemetry from '@crowd/telemetry'

import { IJobDefinition } from '../types'

const STALL_SNAPSHOT_KEY_PREFIX = 'queue-monitoring:partitions'
const STALL_SNAPSHOT_TTL_SECONDS = 2 * 60 * 60
const REDIS_OPERATION_TIMEOUT_MS = 5000

interface IPartitionOffsets {
  committed: number
  lag: number
}

interface IPartitionSnapshot extends IPartitionOffsets {
  stalledSince?: number
}

interface ITopicSnapshot {
  at: number
  partitions: Record<string, IPartitionSnapshot>
}

const job: IJobDefinition = {
  name: 'queue-monitoring',
  cronTime: CronTime.every(30).minutes(),
  timeout: 30 * 60,
  enabled: async () => IS_PROD_ENV,
  process: async (ctx) => {
    const admin = getKafkaClient(QUEUE_CONFIG()).admin()
    await admin.connect()

    try {
      const alerts = await checkQueues(ctx.log, admin)

      if (alerts.length > 0) {
        await sendSlackNotificationAsync(
          SlackChannel.CDP_CRITICAL_ALERTS,
          SlackPersona.WARNING_PROPAGATOR,
          'Queue Monitoring Alert',
          alerts.join('\n'),
        )
        ctx.log.info({ alerts: alerts.length }, 'Queue monitoring alert sent to Slack!')
      }
    } finally {
      await admin.disconnect()
    }

    telemetry.flush()
    // metrics are sent asynchronously after flush, so give them time before the job exits
    await timeout(30000)
  },
}

async function checkQueues(log: Logger, admin: KafkaAdmin): Promise<string[]> {
  const ignoredTopics = getIgnoredTopics()
  const groupsByTopic = await findConsumerGroupsByTopic(admin)
  const redis = await connectRedis(log)
  const alerts = redis ? [] : ['Stall check skipped: Redis is unavailable!']

  for (const [topic, groups] of groupsByTopic) {
    const totalMessages = await countTopicMessages(admin, topic)
    telemetry.gauge(`kafka.${topic}.total`, totalMessages)

    for (const group of groups) {
      const { consumed, unconsumed } = await getKafkaMessageCounts(log, admin, topic, group)
      telemetry.gauge(`kafka.${topic}.${group}.consumed`, consumed)
      telemetry.gauge(`kafka.${topic}.${group}.unconsumed`, unconsumed)
    }

    if (ignoredTopics.has(topic)) {
      continue
    }

    if (groups.length === 0) {
      alerts.push(
        `No consumer groups found for topic ${topic}! Total messages in topic: ${totalMessages}`,
      )
    }

    const consumedByWorker = groups.includes(topic)
    if (redis && consumedByWorker) {
      const stalled = await findStalledPartitions(log, admin, redis, topic)
      if (stalled) {
        alerts.push(stalled)
      }
    }
  }

  return alerts
}

function getIgnoredTopics(): Set<string> {
  const topics = process.env.CROWD_KAFKA_STREAMS_TO_IGNORE_ON_MONITORING?.split(',') ?? []
  return new Set(topics.map((topic) => topic.trim()))
}

async function findConsumerGroupsByTopic(admin: KafkaAdmin): Promise<Map<string, string[]>> {
  const topics = await admin.listTopics()
  const { groups } = await admin.listGroups()
  const { groups: descriptions } = await admin.describeGroups(
    distinct(groups.map(({ groupId }) => groupId)),
  )

  const groupsByTopic = new Map(topics.map((topic) => [topic, [] as string[]]))
  for (const group of descriptions) {
    for (const topic of await findGroupTopics(admin, group, topics)) {
      groupsByTopic.get(topic)?.push(group.groupId)
    }
  }

  return groupsByTopic
}

async function findGroupTopics(
  admin: KafkaAdmin,
  group: KafkaGroupDescription,
  topics: string[],
): Promise<string[]> {
  if (group.state === 'Dead') {
    return []
  }

  if (group.protocolType === 'consumer' && group.members.length > 0) {
    return distinct(
      group.members.flatMap(
        ({ memberMetadata }) => KafkaAssignerProtocol.MemberMetadata.decode(memberMetadata).topics,
      ),
    )
  }

  const offsets = await admin.fetchOffsets({ groupId: group.groupId, topics })
  return offsets
    .filter(({ partitions }) => partitions.some(({ offset }) => offset !== '-1'))
    .map(({ topic }) => topic)
}

async function countTopicMessages(admin: KafkaAdmin, topic: string): Promise<number> {
  const partitions = await admin.fetchTopicOffsets(topic)
  return partitions.reduce(
    (total, { offset, low }) => total + Math.max(0, Number(offset) - Number(low)),
    0,
  )
}

async function findStalledPartitions(
  log: Logger,
  admin: KafkaAdmin,
  redis: RedisClient,
  topic: string,
): Promise<string | undefined> {
  try {
    const key = `${STALL_SNAPSHOT_KEY_PREFIX}:${topic}`
    const stored = await withRedisTimeout(redis.get(key))
    const previous: ITopicSnapshot | undefined = stored ? JSON.parse(stored) : undefined

    const now = Date.now()
    const current: ITopicSnapshot = { at: now, partitions: {} }
    const stalledPartitions: string[] = []
    let stalledLag = 0
    let stalledSince = now

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

    await withRedisTimeout(
      redis.set(key, JSON.stringify(current), { EX: STALL_SNAPSHOT_TTL_SECONDS }),
    )

    if (stalledPartitions.length === 0) {
      return undefined
    }

    log.warn({ topic, stalledPartitions, stalledLag }, 'Consumer group stopped consuming!')
    const totalPartitions = Object.keys(current.partitions).length
    return (
      `Consumer group ${topic}: ${stalledPartitions.length} of ${totalPartitions} partitions consumed ` +
      `nothing since ${new Date(stalledSince).toISOString()} (${formatDuration(now - stalledSince)}) ` +
      `while ${stalledLag.toLocaleString('en-US')} messages wait on them!`
    )
  } catch (err) {
    log.error(err, `Failed to check topic ${topic} for stalled partitions!`)
    return `Stall check for topic ${topic} failed: ${err.message}`
  }
}

async function readPartitionOffsets(
  admin: KafkaAdmin,
  topic: string,
): Promise<Record<string, IPartitionOffsets>> {
  const topicOffsets = await admin.fetchTopicOffsets(topic)
  const groupOffsets = await admin.fetchOffsets({ groupId: topic, topics: [topic] })

  const offsets: Record<string, IPartitionOffsets> = {}
  for (const { partition, offset } of groupOffsets[0]?.partitions ?? []) {
    const topicOffset = topicOffsets.find((o) => o.partition === partition)
    if (!topicOffset || offset === '-1') {
      continue
    }

    const committed = Number(offset)
    const high = Number(topicOffset.offset)
    const low = Number(topicOffset.low)
    offsets[partition] = { committed, lag: Math.max(0, high - Math.max(committed, low)) }
  }

  return offsets
}

function formatDuration(ms: number): string {
  const minutes = Math.round(ms / 60000)
  return minutes < 60 ? `${minutes} min` : `${Math.floor(minutes / 60)} h ${minutes % 60} min`
}

async function connectRedis(log: Logger): Promise<RedisClient | undefined> {
  try {
    // without a listener a dropped connection is an unhandled 'error' event that kills the job
    return await withRedisTimeout(
      getRedisClient(REDIS_CONFIG(), false, (err) =>
        log.warn(err, 'Redis client error during queue monitoring!'),
      ),
    )
  } catch (err) {
    log.error(err, 'Failed to connect to Redis, skipping the consumer stall check!')
    return undefined
  }
}

async function withRedisTimeout<T>(promise: Promise<T>): Promise<T> {
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

export default job
