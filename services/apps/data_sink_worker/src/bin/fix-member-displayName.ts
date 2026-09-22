import { normalizeDisplayName } from '@crowd/common'
import { SearchSyncWorkerEmitter } from '@crowd/common_services'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  getMembersWithWrongDisplayName,
  updateMemberDisplayName,
} from '@crowd/data-access-layer/src/old/apps/data_sink_worker/scripts/fix-member-displayName'
import { getServiceLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'

import { DB_CONFIG, QUEUE_CONFIG } from '../conf'

/* eslint-disable @typescript-eslint/no-explicit-any */

const log = getServiceLogger()

setImmediate(async () => {
  const dbClient = await getDbConnection(DB_CONFIG())
  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())

  log.info('Started fixing members displayName!')

  let fixableMembers
  let processedMembers = 0

  const totalWrongMembers = (await getMembersWithWrongDisplayName(dbClient, { countOnly: true }))
    .count

  if (totalWrongMembers == 0) {
    log.info('No members with incorrect displayName found!')
    process.exit(0)
  }

  log.info(`Found ${totalWrongMembers} members!`)

  const searchSyncWorkerEmitter = new SearchSyncWorkerEmitter(queueClient, log)
  await searchSyncWorkerEmitter.init()

  while (processedMembers < totalWrongMembers) {
    const result = await getMembersWithWrongDisplayName(dbClient, {})
    fixableMembers = result.rows

    for (const member of fixableMembers) {
      const displayName = normalizeDisplayName(member.displayName)
      await updateMemberDisplayName(dbClient, member.id, displayName)

      searchSyncWorkerEmitter.triggerMemberSync(member.id, false)

      processedMembers += 1
    }

    log.info(`Processed ${processedMembers}/${totalWrongMembers} members`)
  }

  log.info('Finished fixing members with incorrect displayName!')

  process.exit(0)
})
