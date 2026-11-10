import { markOrganizationsChanged } from '@crowd/common_services'
import OrganizationRepository from '@crowd/data-access-layer/src/old/apps/script_executor_worker/organization.repo'
import { OrganizationSyncService } from '@crowd/opensearch'

import { svc } from '../../main'

export async function getOrganizationsToCleanup(batchSize: number): Promise<string[]> {
  try {
    const orgRepo = new OrganizationRepository(svc.postgres.reader.connection(), svc.log)
    return orgRepo.getOrganizationsForCleanup(batchSize)
  } catch (error) {
    svc.log.error(error, 'Error getting organizations for cleanup!')
    throw error
  }
}

export async function deleteOrganization(orgId: string): Promise<void> {
  try {
    const orgRepo = new OrganizationRepository(svc.postgres.writer.connection(), svc.log)
    await orgRepo.cleanupOrganization(orgId)
    await markOrganizationsChanged(svc.redis, [orgId], svc.log)
  } catch (error) {
    svc.log.error(error, 'Error cleaning up organization in database!')
    throw error
  }
}

export async function syncRemoveOrganization(organizationId: string): Promise<void> {
  try {
    const service = new OrganizationSyncService(svc.postgres.writer, svc.opensearch, svc.log)
    await service.removeOrganization(organizationId)
  } catch (error) {
    svc.log.error(error, 'Error removing organization in opensearch!')
    throw error
  }
}

export async function queueOrgForAggComputation(orgId: string): Promise<void> {
  try {
    await svc.redis.sAdd('organizationIdsForAggComputation', orgId)
  } catch (error) {
    svc.log.error(error, 'Error adding organization to redis set!')
    throw error
  }
}
