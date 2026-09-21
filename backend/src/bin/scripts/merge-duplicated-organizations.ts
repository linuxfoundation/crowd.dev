import { QueryTypes } from 'sequelize'

import SegmentRepository from '@/database/repositories/segmentRepository'
import { getServiceLogger } from '@crowd/logging'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'
import OrganizationService from '../../services/organizationService'

/* eslint-disable no-continue */
/* eslint-disable @typescript-eslint/no-loop-func */

const log = getServiceLogger()

async function mergeOrganizationsWithSameWebsite(): Promise<void> {
  const dbOptions = await SequelizeRepository.getDefaultIRepositoryOptions()

  const BATCH_SIZE = 500

  let offset
  let mergeableOrganizations

  const seq = SequelizeRepository.getSequelize(dbOptions)

  log.info('Querying database for organizations with same website in a tenant..')

  do {
    offset = mergeableOrganizations ? offset + BATCH_SIZE : 0
    mergeableOrganizations = await seq.query(
      `
        select 
          array_agg(o.id order by o."createdAt" asc) as "organizationIds", 
          o.website, 
          o."tenantId" 
        from organizations o
        where o.website is not null and o."deletedAt" is null
        group by o."tenantId", o.website
        having count(o.id) > 1
        limit ${BATCH_SIZE}
        offset ${offset};`,
      {
        type: QueryTypes.SELECT,
      },
    )

    log.info(`Found ${mergeableOrganizations.length} mergeable organizations.`)

    for (const orgInfo of mergeableOrganizations) {
      const segmentRepository = new SegmentRepository({
        ...dbOptions,
        currentTenant: {
          id: orgInfo.tenantId,
        },
      })

      const segments = (await segmentRepository.querySubprojects({ limit: null, offset: 0 })).rows

      const service = new OrganizationService({
        ...dbOptions,
        currentTenant: {
          id: orgInfo.tenantId,
        },
        currentSegments: segments,
      })

      const primaryOrganizationId = orgInfo.organizationIds.shift()
      for (const orgId of orgInfo.organizationIds) {
        log.info(`Merging organization ${orgId} into ${primaryOrganizationId}!`)
        await service.mergeSync(primaryOrganizationId, orgId, null)
      }
    }
  } while (mergeableOrganizations.length > 0)
}

setImmediate(async () => {
  log.info('Starting merging organizations with same website!')
  await mergeOrganizationsWithSameWebsite()
})
