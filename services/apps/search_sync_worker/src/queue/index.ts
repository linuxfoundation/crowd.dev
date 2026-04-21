import { DbConnection, DbStore } from '@crowd/data-access-layer/src/database'
import { Logger } from '@crowd/logging'
import { MemberSyncService, OpenSearchService, OrganizationSyncService } from '@crowd/opensearch'
import { CrowdQueue, IQueue, PrioritizedQueueReciever } from '@crowd/queue'
import { RedisClient } from '@crowd/redis'
import { IQueueMessage, QueuePriorityLevel, SearchSyncWorkerQueueMessageType } from '@crowd/types'

/* eslint-disable @typescript-eslint/no-explicit-any */
export class WorkerQueueReceiver extends PrioritizedQueueReciever {
  // private readonly memberBatchProcessor: BatchProcessor<string>
  // private readonly organizationBatchProcessor: BatchProcessor<string>

  constructor(
    level: QueuePriorityLevel,
    private readonly redisClient: RedisClient,
    client: IQueue,
    private readonly pgConn: DbConnection,
    private readonly openSearchService: OpenSearchService,
    parentLog: Logger,
    maxConcurrentProcessing: number,
  ) {
    super(
      level,
      client,
      client.getQueueChannelConfig(CrowdQueue.SEARCH_SYNC_WORKER),
      maxConcurrentProcessing,
      parentLog,
      true,
      5 * 60,
      10,
    )

    // this.memberBatchProcessor = new BatchProcessor(
    //   100,
    //   30,
    //   async (memberIds) => {
    //     const distinct = Array.from(new Set(memberIds))
    //     if (distinct.length > 0) {
    //       this.log.info({ batchSize: distinct.length }, 'Processing batch of members!')
    //       await this.initMemberService().syncMembers(distinct)
    //     }
    //   },
    //   async (memberIds, err) => {
    //     this.log.error(err, { memberIds }, 'Error while processing batch of members!')
    //   },
    // )

    // this.organizationBatchProcessor = new BatchProcessor(
    //   20,
    //   30,
    //   async (organizationIds) => {
    //     const distinct = Array.from(new Set(organizationIds))
    //     if (distinct.length > 0) {
    //       this.log.info({ batchSize: distinct.length }, 'Processing batch of organizations!')
    //       await this.initOrganizationService().syncOrganizations(distinct)
    //     }
    //   },
    //   async (organizationIds, err) => {
    //     this.log.error(err, { organizationIds }, 'Error while processing batch of organizations!')
    //   },
    // )
  }

  private initMemberService(): MemberSyncService {
    return new MemberSyncService(
      this.redisClient,
      new DbStore(this.log, this.pgConn),
      this.openSearchService,
      this.log,
    )
  }

  private initOrganizationService(): OrganizationSyncService {
    return new OrganizationSyncService(
      new DbStore(this.log, this.pgConn),
      this.openSearchService,
      this.log,
    )
  }

  public override async processMessage<T extends IQueueMessage>(message: T): Promise<void> {
    try {
      this.log.trace({ messageType: message.type }, 'Processing message!')

      const type = message.type as SearchSyncWorkerQueueMessageType
      const data = message as any

      switch (type) {
        // members
        case SearchSyncWorkerQueueMessageType.SYNC_MEMBER:
          if (data.memberId) {
            // await this.memberBatchProcessor.addToBatch(data.memberId)
            await this.initMemberService().syncMembers(data.memberId)
          }

          break
        case SearchSyncWorkerQueueMessageType.SYNC_ORGANIZATION_MEMBERS:
          if (data.organizationId) {
            this.initMemberService()
              .syncOrganizationMembers(data.organizationId)
              .catch((err) => this.log.error(err, 'Error while syncing organization members!'))
          }

          break
        // this one taks a while so we can't relly on it to be finished in time and the queue message might pop up again so we immediatelly return
        case SearchSyncWorkerQueueMessageType.CLEANUP_MEMBERS:
          this.initMemberService()
            .cleanupMemberIndex()
            .catch((err) => this.log.error(err, 'Error while cleaning up members!'))

          break
        case SearchSyncWorkerQueueMessageType.REMOVE_MEMBER:
          if (data.memberId) {
            await this.initMemberService().removeMember(data.memberId)
          }
          break

        // organizations
        case SearchSyncWorkerQueueMessageType.SYNC_ORGANIZATION:
          if (data.organizationId) {
            // await this.organizationBatchProcessor.addToBatch(data.organizationId)
            await this.initOrganizationService().syncOrganizations([data.organizationId])
          }
          break
        case SearchSyncWorkerQueueMessageType.CLEANUP_ORGANIZATIONS:
          this.initOrganizationService()
            .cleanupOrganizationIndex()
            .catch((err) => {
              this.log.error(err, 'Error while cleaning up all organizations!')
            })
          break
        case SearchSyncWorkerQueueMessageType.REMOVE_ORGANIZATION:
          if (data.organizationId) {
            await this.initOrganizationService().removeOrganization(data.organizationId)
          }
          break

        default:
          throw new Error(`Unknown message type: ${message.type}`)
      }
    } catch (err) {
      this.log.error(err, 'Error while processing message!')
      throw err
    }
  }
}
