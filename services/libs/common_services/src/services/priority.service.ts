import { IS_DEV_ENV, IS_STAGING_ENV, IS_TEST_ENV } from '@crowd/common'
import { Logger, getChildLogger } from '@crowd/logging'
import { CrowdQueue, IQueue, IQueueConfig, PrioritizedQueueEmitter } from '@crowd/queue'
import { IQueueMessage, IQueuePriorityCalculationContext, QueuePriorityLevel } from '@crowd/types'

export class QueuePriorityService {
  private readonly log: Logger

  private readonly emitter: PrioritizedQueueEmitter

  public constructor(
    public readonly queue: CrowdQueue,
    private readonly queueConfig: IQueueConfig,
    private readonly client: IQueue,
    parentLog: Logger,
  ) {
    this.log = getChildLogger(this.constructor.name, parentLog)
    this.emitter = new PrioritizedQueueEmitter(this.client, this.queueConfig, this.log)
  }

  public isInitialized(): boolean {
    return this.emitter.isInitialized()
  }

  public async init(): Promise<void> {
    await this.emitter.init()
  }

  public async setMessageVisibilityTimeout(
    receiptHandle: string,
    newVisibility: number,
  ): Promise<void> {
    const priorityLevel = await this.getPriorityLevel()

    return this.emitter.setMessageVisibilityTimeout(receiptHandle, newVisibility, priorityLevel)
  }

  public async sendMessages<T extends IQueueMessage>(
    messages: {
      payload: T
      groupId: string
      deduplicationId?: string
      id?: string
    }[],
    priorityLevelContextOverride?: IQueuePriorityCalculationContext,
  ): Promise<void> {
    const priorityLevel = await this.getPriorityLevel(priorityLevelContextOverride)

    return this.emitter.sendMessages(
      messages.map((m) => {
        return { ...m, priorityLevel }
      }),
    )
  }

  public async sendMessage<T extends IQueueMessage>(
    groupId: string,
    message: T,
    deduplicationId?: string,
    priorityLevelContextOverride?: unknown,
    priorityLevelOverride?: QueuePriorityLevel,
  ): Promise<void> {
    let priorityLevel = priorityLevelOverride
    if (!priorityLevel) {
      priorityLevel = await this.getPriorityLevel(priorityLevelContextOverride)
    } else if (IS_DEV_ENV || IS_TEST_ENV || IS_STAGING_ENV) {
      priorityLevel = QueuePriorityLevel.NORMAL
    }

    return this.emitter.sendMessage(groupId, message, priorityLevel, deduplicationId)
  }

  private async getPriorityLevel(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    override?: IQueuePriorityCalculationContext,
  ): Promise<QueuePriorityLevel> {
    if (IS_DEV_ENV || IS_TEST_ENV || IS_STAGING_ENV) {
      return QueuePriorityLevel.NORMAL
    }

    const priority = this.calculateQueuePriorityLevel(override)

    return priority
  }

  private calculateQueuePriorityLevel(ctx?: IQueuePriorityCalculationContext): QueuePriorityLevel {
    if (!ctx) {
      return QueuePriorityLevel.NORMAL
    }

    if (ctx.dbPriority) {
      return ctx.dbPriority
    }

    if (ctx.onboarding) {
      return QueuePriorityLevel.HIGH
    }

    return QueuePriorityLevel.NORMAL
  }
}
