import { IS_PROD_ENV, IS_STAGING_ENV } from '@crowd/common'
import { Logger, LoggerBase } from '@crowd/logging'
import { IQueueMessage, IQueueMessageBulk, QueuePriorityLevel } from '@crowd/types'

import { IQueue, IQueueChannel, IQueueConfig, IQueueInitChannelConfig } from './types'

export abstract class QueueBase extends LoggerBase {
  private readonly channelName: string
  private channelUrl: string | undefined

  constructor(
    public readonly queue: IQueue,
    public readonly queueConf: IQueueConfig,
    parentLog: Logger,
  ) {
    super(parentLog, {
      queueName: queueConf.name,
    })

    if (queueConf.useOnlyNameAsChannel) {
      this.channelName = queueConf.name
    } else {
      this.channelName = `${queueConf.name}${this.getQueueSuffix()}`
    }
  }

  public isInitialized(): boolean {
    return this.channelUrl !== undefined
  }

  protected getQueueUrl(): string {
    if (this.channelUrl) {
      return this.channelUrl
    }

    throw new Error('Queue URL not set - please call init() first!')
  }

  protected getQueueName(): string {
    if (this.channelName) {
      return this.channelName
    }

    throw new Error('Queue name not set!')
  }

  protected getChannel(): IQueueChannel {
    return {
      name: this.channelName,
      url: this.channelUrl,
    }
  }

  private getQueueSuffix(): string {
    let queueSuffix = ''
    if (IS_STAGING_ENV) {
      queueSuffix = '-staging'
    } else if (IS_PROD_ENV) {
      queueSuffix = '-production'
    }
    return queueSuffix
  }

  public async init(config: IQueueInitChannelConfig, level?: QueuePriorityLevel): Promise<void> {
    this.channelUrl = await this.queue.init(
      {
        ...config,
        name: this.getChannel().name,
      },
      level,
    )
  }
}

export abstract class QueueReceiver extends QueueBase {
  constructor(
    queue: IQueue,
    queueConf: IQueueConfig,
    private readonly maxConcurrentMessageProcessing: number,
    parentLog: Logger,
    private readonly deleteMessageImmediately = false,
    private readonly visibilityTimeoutSeconds?: number,
    private readonly receiveMessageCount?: number,
  ) {
    super(queue, queueConf, parentLog)
  }

  public async start(queueConf: IQueueConfig, level?: QueuePriorityLevel): Promise<void> {
    await this.queue.init({ ...queueConf, name: this.getChannel().name }, level)
    await this.queue.start(
      this.processMessage,
      this.maxConcurrentMessageProcessing,
      { ...queueConf, name: this.getChannel().name },
      {
        deleteMessageImmediately: this.deleteMessageImmediately,
        visibilityTimeoutSeconds: this.visibilityTimeoutSeconds,
        receiveMessageCount: this.receiveMessageCount,
      },
    )
  }

  public stop() {
    this.queue.stop()
  }

  public receive(channel: IQueueChannel) {
    return this.queue.receive(channel, {
      deleteMessageImmediately: this.deleteMessageImmediately,
      visibilityTimeoutSeconds: this.visibilityTimeoutSeconds,
      receiveMessageCount: this.receiveMessageCount,
    })
  }

  protected abstract processMessage(data: IQueueMessage, receiptHandle?: string): Promise<void>
}

export class QueueEmitter extends QueueBase {
  constructor(queue: IQueue, queueConf: IQueueConfig, parentLog: Logger) {
    super(queue, queueConf, parentLog)
  }

  public async sendMessage<T extends IQueueMessage>(
    groupId: string,
    message: T,
    deduplicationId?: string,
  ): Promise<void> {
    await this.queue.send(this.getChannel(), message, groupId, {
      deduplicationId,
      config: this.queueConf,
    })
  }

  public async sendMessages<T extends IQueueMessage>(
    messages: IQueueMessageBulk<T>[],
  ): Promise<void> {
    await this.queue.sendBulk(this.getChannel(), messages)
  }

  public async setMessageVisibilityTimeout(
    receiptHandle: string,
    newVisibility: number,
  ): Promise<void> {
    await this.queue.setMessageVisibilityTimeout(this.getChannel(), receiptHandle, newVisibility)
  }
}
