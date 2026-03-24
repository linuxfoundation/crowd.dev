import CronTime from 'cron-time-generator'

import { IS_PROD_ENV, distinct, timeout } from '@crowd/common'
import { Logger } from '@crowd/logging'
import { KafkaAdmin, QUEUE_CONFIG, getKafkaClient } from '@crowd/queue'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'
import telemetry from '@crowd/telemetry'

import { IJobDefinition } from '../types'

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
        const counts = await getMessageCounts(ctx.log, admin, topic, group)
        ctx.log.info(
          `Topic ${topic} group ${group} has ${counts.total} total messages, ${counts.consumed} consumed, ${counts.unconsumed} unconsumed!`,
        )

        telemetry.gauge(`kafka.${topic}.${group}.consumed`, counts.consumed)
        telemetry.gauge(`kafka.${topic}.${group}.unconsumed`, counts.unconsumed)
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
          } catch (metadataErr) {
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

async function getMessageCounts(
  log: Logger,
  admin: KafkaAdmin,
  topic: string,
  groupId: string,
): Promise<{
  total: number
  consumed: number
  unconsumed: number
}> {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(topic)
    const offsetsResponse = await admin.fetchOffsets({
      groupId: groupId,
      topics: [topic],
    })

    const offsets = offsetsResponse[0].partitions

    let totalMessages = 0
    let consumedMessages = 0
    let totalLeft = 0

    for (const offset of offsets) {
      const topicOffset = topicOffsets.find((p) => p.partition === offset.partition)
      if (topicOffset) {
        // Total messages is the latest offset
        totalMessages += Number(topicOffset.offset)

        // Handle -1 offsets (no committed offset)
        if (offset.offset === '-1') {
          // No committed offset means no messages consumed from this partition
          consumedMessages += 0
          // Unconsumed is the total messages in the partition
          totalLeft += Number(topicOffset.offset) - Number(topicOffset.low)
        } else {
          // Consumed messages is the consumer group's offset
          consumedMessages += Number(offset.offset)
          // Unconsumed is the difference
          totalLeft += Number(topicOffset.offset) - Number(offset.offset)
        }
      }
    }

    return {
      total: totalMessages,
      consumed: consumedMessages,
      unconsumed: totalLeft,
    }
  } catch (err) {
    log.error(err, 'Failed to get message count!')
    throw err
  }
}

async function getTopicMessageCount(
  log: Logger,
  admin: KafkaAdmin,
  topic: string,
): Promise<number> {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(topic)

    // Sum up all partition offsets to get total messages
    const totalMessages = topicOffsets.reduce((sum, partition) => {
      return sum + Number(partition.offset)
    }, 0)

    return totalMessages
  } catch (err) {
    log.error(err, `Failed to get message count for topic ${topic}!`)
    throw err
  }
}

export default job
