import { mapWithConcurrency } from '@crowd/common'
import { fetchOrganizationsForSearchSync, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import {
  OpenSearchIndex,
  OpenSearchService,
  OrganizationSyncService,
  buildFullOrgForMergeSuggestions,
  getOpensearchClient,
} from '@crowd/opensearch'
import { RedisCache, getRedisClient } from '@crowd/redis'

import { DB_CONFIG, OPENSEARCH_CONFIG, REDIS_CONFIG } from '../conf'

const BATCH_SIZE = 500
const CONCURRENCY = 10
const CHECKPOINT_KEY = 'organizations'

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

  let lastOrganizationId = (await checkpoint.get(CHECKPOINT_KEY)) ?? undefined
  let processed = 0

  log.info(
    { lastOrganizationId, BATCH_SIZE, CONCURRENCY },
    'Syncing all organizations to OpenSearch!',
  )

  let organizations = await fetchOrganizationsForSearchSync(qx, BATCH_SIZE, lastOrganizationId)

  while (organizations.length > 0) {
    const documents = await mapWithConcurrency(organizations, CONCURRENCY, async (organization) => {
      try {
        const data = await buildFullOrgForMergeSuggestions(qx, organization)
        return { id: organization.id, body: OrganizationSyncService.prefixData(data) }
      } catch (err) {
        log.error(
          { organizationId: organization.id, err },
          'Failed to build organization document, skipping!',
        )
        return null
      }
    })

    const indexable = documents.filter((document) => document !== null)
    if (indexable.length > 0) {
      await openSearch.bulkIndex(OpenSearchIndex.ORGANIZATIONS, indexable)
    }

    lastOrganizationId = organizations[organizations.length - 1].id
    await checkpoint.set(CHECKPOINT_KEY, lastOrganizationId)

    processed += organizations.length
    log.info({ processed, lastOrganizationId }, 'Synced batch of organizations!')

    organizations = await fetchOrganizationsForSearchSync(qx, BATCH_SIZE, lastOrganizationId)
  }

  await checkpoint.delete(CHECKPOINT_KEY)

  log.info({ processed }, 'Finished syncing all organizations!')
  process.exit(0)
})
