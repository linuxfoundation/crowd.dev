import { DataSinkWorkerEmitter, SearchSyncWorkerEmitter } from '@crowd/common_services'
import { dbStoreQx, findIdentitiesForMembers } from '@crowd/data-access-layer'
import { DbStore, getDbConnection } from '@crowd/data-access-layer/src/database'
import DataSinkRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/dataSink.repo'
import MemberRepository from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/member.repo'
import { getServiceLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'
import { getRedisClient } from '@crowd/redis'
import { Client as TemporalClient, getTemporalClient } from '@crowd/temporal'
import { MemberIdentityType } from '@crowd/types'

import { DB_CONFIG, QUEUE_CONFIG, REDIS_CONFIG, TEMPORAL_CONFIG } from '../conf'
import MemberService from '../service/member.service'
import { OrganizationService } from '../service/organization.service'

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

if (processArguments.length !== 1) {
  log.error('Expected 1 argument: memberId')
  process.exit(1)
}

const memberId = processArguments[0]

setImmediate(async () => {
  let temporal: TemporalClient | undefined
  if (TEMPORAL_CONFIG().serverUrl) {
    temporal = await getTemporalClient(TEMPORAL_CONFIG())
  }

  const redis = await getRedisClient(REDIS_CONFIG())

  const dbConnection = await getDbConnection(DB_CONFIG())
  const store = new DbStore(log, dbConnection)

  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())
  const emitter = new DataSinkWorkerEmitter(queueClient, log)
  await emitter.init()

  const dataSinkRepo = new DataSinkRepository(store, log)
  const memberRepo = new MemberRepository(store, log)

  const searchSyncWorkerEmitter = new SearchSyncWorkerEmitter(queueClient, log)
  await searchSyncWorkerEmitter.init()

  const memberService = new MemberService(store, redis, temporal, log)
  const orgService = new OrganizationService(store, log)

  try {
    const members = await memberRepo.findByIds([memberId])

    const member = members[0]

    if (!member) {
      log.error({ memberId }, 'Member not found!')
      process.exit(1)
    }

    const identities = (await findIdentitiesForMembers(dbStoreQx(store), [memberId])).get(memberId)
    log.info(`Processing memberId: ${member.id}`)

    const segmentIds = await dataSinkRepo.getSegmentIds()
    const segmentId = segmentIds[segmentIds.length - 1] // leaf segment id

    const emailIdentities = identities.filter(
      (i) => i.verified && i.type === MemberIdentityType.EMAIL,
    )
    if (emailIdentities.length > 0) {
      const emails = emailIdentities.map((i) => i.value)
      log.info({ memberId, emails }, 'Member emails!')
      const orgs = await memberService.assignOrganizationByEmailDomain(null, emails)

      if (orgs.length > 0) {
        log.info('Organizations found with matching email domains:', JSON.stringify(orgs))
        orgService.addToMember([segmentId], member.id, orgs)

        for (const org of orgs) {
          await searchSyncWorkerEmitter.triggerOrganizationSync(org.id, true, segmentId)
        }

        await searchSyncWorkerEmitter.triggerMemberSync(member.id, true, segmentId)
        log.info('Done mapping member to organizations!')
      } else {
        log.info('No organizations found with matching email domains!')
      }
    } else {
      log.info('No emails found for member!')
    }

    process.exit(0)
  } catch (err) {
    log.error('Failed to map organizations for member!', err)
    process.exit(1)
  }
})
