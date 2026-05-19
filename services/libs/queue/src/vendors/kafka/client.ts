/* eslint-disable @typescript-eslint/no-unused-vars */
import { createHash } from 'crypto'
import { Admin, Consumer, EachMessagePayload, Kafka, KafkaMessage, Producer } from 'kafkajs'

import { SERVICE, groupBy, timeout } from '@crowd/common'
import { Logger, LoggerBase } from '@crowd/logging'
import telemetry from '@crowd/telemetry'
import { IQueueMessage, IQueueMessageBulk, QueuePriorityLevel } from '@crowd/types'

import {
  CrowdQueue,
  IQueue,
  IQueueChannel,
  IQueueProcessMessageHandler,
  IQueueSendBulkResult,
  IQueueSendResult,
} from '../../types'

import { configMap } from './config'
import { IKafkaChannelConfig, IKafkaQueueStartOptions } from './types'
import { getKafkaMessageCounts } from './utils'

export class KafkaQueueService extends LoggerBase implements IQueue {
  private readonly MAX_RECONNECT_ATTEMPTS = 5
  private readonly RECONNECT_DELAY = 5000

  private reconnectAttempts: Map<string, number>
  private consumerStatus: Map<string, boolean>
  private consumers: Map<string, Consumer>
  private processingMessages: number
  private started: boolean
  private producer?: Producer | undefined = undefined

  public constructor(
    public readonly client: Kafka,
    parentLog: Logger,
  ) {
    super(parentLog, {
      service: 'kafka-queue',
    })
    this.processingMessages = 0
    this.started = false
    this.consumers = new Map<string, Consumer>()
    this.reconnectAttempts = new Map<string, number>()
    this.consumerStatus = new Map<string, boolean>()
  }

  async getProducer(): Promise<Producer> {
    if (!this.producer) {
      const producer = this.client.producer()
      await producer.connect()
      this.producer = producer
    }

    return this.producer
  }

  async getQueueMessageCount(conf: IKafkaChannelConfig): Promise<number> {
    const topic = conf.name
    const groupId = topic

    const admin = this.client.admin()
    await admin.connect()

    try {
      const counts = await getKafkaMessageCounts(this.log, admin, topic, groupId)
      return counts.unconsumed
    } catch (err) {
      this.log.error({ topic, groupId, err }, 'Failed to get message count!')
      throw err
    } finally {
      await admin.disconnect()
    }
  }

  public async send(
    channel: IQueueChannel,
    message: IQueueMessage,
    groupId: string,
  ): Promise<IQueueSendResult> {
    telemetry.increment('kafka.send', 1, {
      topic: channel.name,
      type: message.type,
      service: SERVICE,
    })

    const producer = await this.getProducer()
    // send message to kafka
    const result = await producer.send({
      topic: channel.name,
      messages: [
        {
          key: groupId,
          value: JSON.stringify(message),
        },
      ],
    })

    this.log.trace({ message: message, topic: channel.name }, 'Message sent to Kafka topic!')

    return result
  }

  async receive(channel: IQueueChannel): Promise<KafkaMessage[]> {
    const consumer = await this.getConsumer(channel.name)
    await consumer.subscribe({ topic: channel.name, fromBeginning: true })
    return new Promise<KafkaMessage[]>((resolve) => {
      consumer.run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          resolve([message])
          await consumer.disconnect()
          this.consumers.delete(channel.name)
        },
      })
    })
  }

  private async getConsumer(groupId: string): Promise<Consumer> {
    if (!this.consumers.get(groupId)) {
      const consumer = this.client.consumer({
        groupId,
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        heartbeatInterval: 3000,
      })
      consumer.on(consumer.events.GROUP_JOIN, () => {
        this.log.info('Consumer has joined the group')
      })

      consumer.on(consumer.events.REBALANCING, () => {
        this.log.info('Consumer group is rebalancing')
      })

      consumer.on(consumer.events.DISCONNECT, async () => {
        this.log.warn('Consumer disconnected, attempting reconnection')
        await this.handleConsumerError(groupId, consumer)
      })
      consumer.on(consumer.events.COMMIT_OFFSETS, (event) => {
        if (process.env.KAFKA_LOG_COMMITS) {
          this.log.info(`Consumer committed offsets: ${JSON.stringify(event, null, 2)}`)
        }
      })
      consumer.on(consumer.events.CRASH, async (event) => {
        this.log.error({ error: event.payload.error }, 'Consumer crashed')
        await this.handleConsumerError(groupId, consumer)
      })
      this.consumers.set(groupId, consumer)
      await this.connectConsumer(consumer)

      const origCommit = consumer.commitOffsets.bind(consumer)
      consumer.commitOffsets = async function (data) {
        if (process.env.KAFKA_LOG_COMMITS) {
          this.log.info(`Consumer committing offsets: ${JSON.stringify(data, null, 2)}`)
        }

        const res = await origCommit(data)

        if (process.env.KAFKA_LOG_COMMITS) {
          this.log.info(`Consumer commited offsets: ${JSON.stringify(data, null, 2)}`)
        }

        return res
      }
    }

    return this.consumers.get(groupId)
  }

  private async handleConsumerError(groupId: string, consumer: Consumer) {
    if (this.consumerStatus.has(groupId)) {
      // do nothing we are already rejoining
      return
    }

    this.consumerStatus.set(groupId, true)
    const attempts = this.reconnectAttempts.get(groupId) || 0

    if (attempts < this.MAX_RECONNECT_ATTEMPTS) {
      this.reconnectAttempts.set(groupId, attempts + 1)
      this.log.info(
        `Attempting to reconnect consumer (attempt ${attempts + 1}/${this.MAX_RECONNECT_ATTEMPTS})`,
      )

      try {
        await timeout(this.RECONNECT_DELAY)
        await this.connectConsumer(consumer)
        this.reconnectAttempts.set(groupId, 0) // Reset attempts on successful reconnection
      } catch (error) {
        this.log.error({ error }, 'Failed to reconnect consumer')
        await this.handleConsumerError(groupId, consumer)
      } finally {
        this.consumerStatus.delete(groupId)
      }
    } else {
      this.log.error(
        `Max reconnection attempts (${this.MAX_RECONNECT_ATTEMPTS}) reached for consumer ${groupId}`,
      )

      // try to gracefully shutdown but with an error code so that the service is restarted
      this.stop()
      await timeout(1000)
      process.exit(1)
    }
  }

  private async connectConsumer(consumer: Consumer) {
    try {
      await consumer.connect()
      this.log.info('Consumer connected!')
    } catch (err) {
      this.log.error(err, 'Failed to connect consumer!')
      throw err
    }
  }

  public getClient() {
    return this.client
  }

  public async init(config: IKafkaChannelConfig, level?: QueuePriorityLevel): Promise<string> {
    this.log.info({ config }, 'Initializing queue!')

    const admin = this.client.admin()
    await admin.connect()

    let partitionCount

    if (level && config.partitions[level]) {
      partitionCount = config.partitions[level]
    } else if (config.partitions.default) {
      partitionCount = config.partitions.default
    } else {
      partitionCount = 1
    }

    try {
      const existingTopics = await admin.listTopics()
      if (!existingTopics.includes(config.name)) {
        await admin.createTopics({
          topics: [
            {
              topic: config.name,
              numPartitions: partitionCount,
              replicationFactor: config.replicationFactor || 1,
              configEntries: [
                {
                  name: 'retention.ms',
                  value: '604800000', // 7 days in milliseconds
                },
              ],
            },
          ],
          waitForLeaders: false,
        })
        this.log.info(
          `Topic "${config.name}" created with ${partitionCount} partitions and ${
            config.replicationFactor || 1
          } replication factor.`,
        )
      }

      await this.waitForTopicAvailability(admin, config.name)

      // Init function should return the channel url, but in kafka
      // there is no such thing as a channel/topic url, so we just return the topic name here
      return config.name
    } catch (error) {
      this.log.error(`Failed to create topic "${config.name}":`, error)
    } finally {
      this.log.info(`Queue initialized succesfully  for "${config.name}"!`)
      await admin.disconnect()
    }
  }

  private async waitForTopicAvailability(
    admin: Admin,
    topic: string,
    retries = 5,
    delay = 1000,
  ): Promise<void> {
    for (let i = 0; i < retries; i++) {
      try {
        const metadata = await admin.fetchTopicMetadata({ topics: [topic] })
        if (metadata.topics.length > 0 && metadata.topics[0].partitions.length > 0) {
          this.log.info(`Topic "${topic}" is now available.`)
          return
        }
      } catch (e) {
        this.log.trace(`Error fetching metadata for topic "${topic}":`, e)
      }

      this.log.info(`Waiting for topic "${topic}" to become available... (${i + 1}/${retries})`)
      await new Promise((resolve) => setTimeout(resolve, delay))
    }
    throw new Error(`Topic "${topic}" did not become available within the expected time.`)
  }

  private isAvailable(maxConcurrentMessageProcessing): boolean {
    return this.processingMessages < maxConcurrentMessageProcessing
  }

  private addJob() {
    this.processingMessages++
  }

  private removeJob() {
    this.processingMessages--
  }

  public async delete(channel: IQueueChannel, options?: unknown): Promise<void> {
    // do nothing
  }

  public async setMessageVisibilityTimeout(
    channel: IQueueChannel,
    handle: string,
    newTimeout: number,
  ): Promise<void> {
    // do nothing
  }

  public async sendBulk(
    channel: IQueueChannel,
    messages: IQueueMessageBulk<IQueueMessage>[],
  ): Promise<IQueueSendBulkResult> {
    for (const [type, data] of groupBy(messages, (m) => m.payload.type)) {
      telemetry.increment('kafka.send', data.length, {
        topic: channel.name,
        type,
        service: SERVICE,
      })
    }

    const producer = await this.getProducer()

    const result = await producer.send({
      topic: channel.name,
      messages: messages.map((m) => ({
        value: JSON.stringify(m.payload),
        key: m.groupId ?? undefined,
      })),
    })

    this.log.debug({ messages, topic: channel.name }, 'Messages sent to Kafka topic!')

    return result
  }

  public async start(
    processMessage: IQueueProcessMessageHandler,
    maxConcurrentMessageProcessing,
    queueConf: IKafkaChannelConfig,
    options?: IKafkaQueueStartOptions,
  ): Promise<void> {
    const MAX_RETRY_FOR_CONNECTING_CONSUMER = 10
    const RETRY_DELAY = 2000
    let retries = options?.retry || 0

    let healthCheckInterval
    let statisticsInterval

    try {
      this.started = true
      this.log.info({ topic: queueConf.name }, 'Starting listening to Kafka topic...')

      const consumer = await this.getConsumer(queueConf.name)
      await consumer.subscribe({ topic: queueConf.name })

      // Add periodic health check
      healthCheckInterval = setInterval(async () => {
        if (!this.started) {
          clearInterval(healthCheckInterval)
          return
        }

        try {
          consumer.pause([{ topic: queueConf.name }])
          consumer.resume([{ topic: queueConf.name }])
        } catch (error) {
          this.log.error({ error }, 'Health check failed, attempting reconnection')
          await this.handleConsumerError(queueConf.name, consumer)
        }
      }, 10 * 60000) // Check every 10 minutes

      let timings = []

      statisticsInterval = setInterval(async () => {
        if (!this.started) {
          clearInterval(statisticsInterval)
          return
        }

        try {
          // Reset the timings array and calculate the average processing time
          const durations = [...timings]
          timings = []

          // Get the number of messages left in the queue
          const count = await this.getQueueMessageCount(queueConf)

          let message = `Topic has ${count} messages left!`
          if (durations.length > 0) {
            const average = durations.reduce((a, b) => a + b, 0) / durations.length
            message += ` In the last minute ${durations.length} messages were processed (${(durations.length / 60.0).toFixed(2)} msg/s) - average processing time: ${average.toFixed(2)}ms!`
          }
          this.log.info({ topic: queueConf.name }, message)
        } catch (err) {
          // do nothing
        }
      }, 60000) // check every minute

      this.log.trace({ topic: queueConf.name }, 'Subscribed to topic! Starting the consmer...')

      await consumer.run({
        autoCommitInterval: 10000, // 10 seconds
        eachMessage: async ({ message }) => {
          try {
            if (message && message.value) {
              const data = JSON.parse(message.value.toString())

              const startWait = performance.now()
              while (!this.isAvailable(maxConcurrentMessageProcessing)) {
                const diff = performance.now() - startWait

                if (diff >= 5000 && diff % 10000 <= 100) {
                  this.log.warn(
                    { topic: queueConf.name },
                    `Consumer is waiting for ${diff.toFixed(2)}ms to process the message! Message type '${data.type}'!`,
                  )
                }

                await timeout(100)
              }
              const now = performance.now()

              this.addJob()

              processMessage(data)
                .then(() => {
                  const duration = performance.now() - now
                  timings.push(duration)
                  this.log.debug(`Message processed successfully in ${duration.toFixed(2)}ms!`)
                })
                .catch((err) => {
                  const duration = performance.now() - now
                  timings.push(duration)
                  this.log.error(
                    err,
                    `Message processed unsuccessfully in ${duration.toFixed(2)}ms!`,
                  )
                })
                .finally(() => {
                  this.removeJob()
                })
            }
          } catch (err) {
            this.log.error(err, 'Failed to process the message')
          }
        },
      })
    } catch (e) {
      this.log.trace({ topic: queueConf.name, error: e }, 'Failed to start the queue!')
      clearInterval(healthCheckInterval)
      clearInterval(statisticsInterval)
      if (retries < MAX_RETRY_FOR_CONNECTING_CONSUMER) {
        retries++
        this.log.trace({ topic: queueConf.name, retries }, 'Retrying to start the queue...')
        await timeout(RETRY_DELAY)
        await this.start(processMessage, maxConcurrentMessageProcessing, queueConf, {
          ...options,
          retry: retries,
        })
      } else {
        throw new Error(
          `Failed to start Kafka consumer after ${MAX_RETRY_FOR_CONNECTING_CONSUMER} retries`,
        )
      }
    }

    process.on('SIGINT', async () => {
      this.started = false
      const promises = Array.from(this.consumers.values()).map((c) => c.disconnect())
      await Promise.all(promises)
      this.log.info('Kafka consumers disconnected')
      process.exit(0)
    })
  }

  public stop() {
    this.started = false
    this.consumers.forEach((c) => c.disconnect())
    this.log.info('Kafka consumers disconnected')
  }

  public getMessageBody(message: KafkaMessage): IQueueMessage {
    return JSON.parse(message.value.toString())
  }

  /**
   * There are no actual message ids in Kafka, we'll create one based on the message content.
   **/
  public getMessageId(message: KafkaMessage): string {
    const jsonString = JSON.stringify(message.value)

    const hash = createHash('md5').update(jsonString).digest('hex')

    return hash
  }

  /**
   * There are no receipt handles in Kafka, we'll create one based on the message offset.
   **/
  public getReceiptHandle(message: KafkaMessage): string {
    return message.offset.toString()
  }

  public getQueueChannelConfig(queue: CrowdQueue): IKafkaChannelConfig {
    return configMap[queue]
  }
}
