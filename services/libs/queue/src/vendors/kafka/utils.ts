import { Admin } from 'kafkajs'

import { Logger } from '@crowd/logging'

/**
 * Returns the total, consumed, and unconsumed message counts for a Kafka topic/consumer-group pair.
 *
 * Uses the low watermark as a floor when computing lag so that stale committed offsets
 * (e.g. after a topic is recreated or truncated) do not produce phantom lag.
 * A committed offset of -1 (no commit yet) is treated as if the consumer is at the low watermark.
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
        const high = Number(topicOffset.offset)
        const low = Number(topicOffset.low)
        // Clamp committed to [low, high]: stale commits below the floor are treated as "at floor";
        // commits above the high watermark (topic recreated at lower offsets) are treated as "at high".
        // -1 means no committed offset; treat as at low watermark.
        const committedRaw = offset.offset === '-1' ? low : Number(offset.offset)
        const committed = Math.min(Math.max(committedRaw, low), high)

        totalMessages += Math.max(0, high - low)
        consumedMessages += Math.max(0, committed - low)
        totalLeft += Math.max(0, high - committed)
      }
    }

    return { total: totalMessages, consumed: consumedMessages, unconsumed: totalLeft }
  } catch (err) {
    log.error(err, 'Failed to get Kafka message counts!')
    throw err
  }
}
