import { Admin } from 'kafkajs'

import { Logger } from '@crowd/logging'

/**
 * Returns the total, consumed, and unconsumed message counts for a Kafka topic/consumer-group pair.
 *
 * Handles the `-1` offset edge case: when a consumer group has never committed an offset,
 * Kafka returns `-1`. In that case all messages from the low watermark onward are unconsumed.
 */
export async function getKafkaMessageCounts(
  log: Logger,
  admin: Admin,
  topic: string,
  groupId: string,
): Promise<{ total: number; consumed: number; unconsumed: number }> {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(topic)
    const offsetsResponse = await admin.fetchOffsets({
      groupId,
      topics: [topic],
    })

    const offsets = offsetsResponse[0].partitions

    let totalMessages = 0
    let consumedMessages = 0
    let totalLeft = 0

    for (const offset of offsets) {
      const topicOffset = topicOffsets.find((p) => p.partition === offset.partition)
      if (topicOffset) {
        totalMessages += Number(topicOffset.offset)

        if (offset.offset === '-1') {
          // No committed offset yet — treat all messages from the low watermark as unconsumed.
          consumedMessages += 0
          totalLeft += Number(topicOffset.offset) - Number(topicOffset.low)
        } else {
          consumedMessages += Number(offset.offset)
          totalLeft += Number(topicOffset.offset) - Number(offset.offset)
        }
      }
    }

    return { total: totalMessages, consumed: consumedMessages, unconsumed: totalLeft }
  } catch (err) {
    log.error(err, 'Failed to get Kafka message counts!')
    throw err
  }
}
