import { mapWithConcurrency } from '@crowd/common'
import { fetchMembersForSearchSync, pgpQx } from '@crowd/data-access-layer'
import { DbStore, getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import {
  MemberSyncService,
  OpenSearchIndex,
  OpenSearchService,
  buildFullMemberForMergeSuggestions,
  getOpensearchClient,
} from '@crowd/opensearch'
import { MemberRepository } from '@crowd/opensearch/src/repo/member.repo'
import { RedisCache, getRedisClient } from '@crowd/redis'

import { DB_CONFIG, OPENSEARCH_CONFIG, REDIS_CONFIG } from '../conf'

const BATCH_SIZE = 500
const CONCURRENCY = 10
const CHECKPOINT_KEY = 'members'

const log = getServiceLogger()

setImmediate(async () => {
  const db = await getDbConnection({
    ...DB_CONFIG(),
    host: process.env.CROWD_DB_READ_HOST ?? DB_CONFIG().host,
  })
  const qx = pgpQx(db)
  const redis = await getRedisClient(REDIS_CONFIG(), true)
  const openSearch = new OpenSearchService(log, await getOpensearchClient(OPENSEARCH_CONFIG()))
  const checkpoint = new RedisCache('opensearch-backfill', redis, log)

  const memberRepo = new MemberRepository(redis, new DbStore(log, db), log)
  const attributes = await memberRepo.getMemberAttributes()

  let lastMemberId = (await checkpoint.get(CHECKPOINT_KEY)) ?? undefined
  let processed = 0

  log.info({ lastMemberId, BATCH_SIZE, CONCURRENCY }, 'Syncing all members to OpenSearch!')

  let members = await fetchMembersForSearchSync(qx, BATCH_SIZE, lastMemberId)

  while (members.length > 0) {
    const documents = await mapWithConcurrency(members, CONCURRENCY, async (member) => {
      try {
        const data = await buildFullMemberForMergeSuggestions(qx, member)
        return { id: member.id, body: MemberSyncService.prefixData(data, attributes) }
      } catch (err) {
        log.error({ memberId: member.id, err }, 'Failed to build member document, skipping!')
        return null
      }
    })

    const indexable = documents.filter((document) => document !== null)
    if (indexable.length > 0) {
      await openSearch.bulkIndex(OpenSearchIndex.MEMBERS, indexable)
    }

    lastMemberId = members[members.length - 1].id
    await checkpoint.set(CHECKPOINT_KEY, lastMemberId)

    processed += members.length
    log.info({ processed, lastMemberId }, 'Synced batch of members!')

    members = await fetchMembersForSearchSync(qx, BATCH_SIZE, lastMemberId)
  }

  await checkpoint.delete(CHECKPOINT_KEY)

  log.info({ processed }, 'Finished syncing all members!')
  process.exit(0)
})
