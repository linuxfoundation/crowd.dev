import { Kafka, KafkaMessage, RecordMetadata } from 'kafkajs'

import { IQueueMessage, IQueueMessageBulk, QueuePriorityLevel } from '@crowd/types'

import { IKafkaClientConfig } from './vendors/kafka/types'

export type IQueueClient = Kafka

// export type IQueueConfig = IKafkaConfig
export interface IQueueConfig {
  name: string
  useOnlyNameAsChannel?: boolean
}

export interface IQueueInitChannelConfig {
  name: string
  useOnlyNameAsChannel?: boolean
}

export type IQueueSendResult = RecordMetadata[]

export type IQueueSendBulkResult = RecordMetadata[]

export type IQueueClientConfig = IKafkaClientConfig

export type IQueueReceiveResponse = KafkaMessage

export type IQueueProcessMessageHandler = (
  message: IQueueMessage,
  options?: unknown,
) => Promise<void>

export interface IQueueChannel {
  name: string
  url?: string
}

export interface IQueue {
  client: IQueueClient
  getClient(): IQueueClient
  init(config: IQueueInitChannelConfig, level?: QueuePriorityLevel): Promise<string>
  getQueueChannelConfig(queue: CrowdQueue): IQueueConfig
  start(
    processMessageFunction: IQueueProcessMessageHandler,
    maxConcurrentMessageProcessing: number,
    queueConf: IQueueConfig,
    options?: unknown,
  ): Promise<void>
  stop(): void
  disconnectProducer(): Promise<void>
  send(
    channel: IQueueChannel,
    message: IQueueMessage,
    groupId: string,
    options?: unknown,
  ): Promise<IQueueSendResult>
  sendBulk(
    channel: IQueueChannel,
    messages: IQueueMessageBulk<IQueueMessage>[],
    options?: unknown,
  ): Promise<IQueueSendBulkResult>
  receive(channel: IQueueChannel, options?: unknown): Promise<IQueueReceiveResponse[]>
  delete(channel: IQueueChannel, options?: unknown): Promise<void>
  getMessageBody(message: IQueueReceiveResponse): IQueueMessage
  getMessageId(message: IQueueReceiveResponse): string
  getReceiptHandle(message: IQueueReceiveResponse): string
  setMessageVisibilityTimeout(
    channel: IQueueChannel,
    handle: string,
    newTimeout: number,
    options?: unknown,
  ): Promise<unknown>
}

export enum CrowdQueue {
  INTEGRATION_RUN_WORKER = 'integration-run-worker',
  INTEGRATION_STREAM_WORKER = 'integration-stream-worker',
  DATA_SINK_WORKER = 'data-sink-worker',
  SEARCH_SYNC_WORKER = 'search-sync-worker',
  ACTIVITIES = 'activities',
}

export enum QueueVendor {
  KAFKA = 'kafka',
}
