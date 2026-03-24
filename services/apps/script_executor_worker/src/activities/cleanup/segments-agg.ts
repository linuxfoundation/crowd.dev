import {
  deleteOrphanMemberSegmentsAgg as deleteMemberSegmentsAgg,
  getOrphanMemberSegmentsAgg as getMemberSegmentsAgg,
} from '@crowd/data-access-layer/src/members/segmentsAgg'
import {
  deleteOrphanOrganizationSegmentsAgg as deleteOrganizationSegmentsAgg,
  getOrphanOrganizationSegmentsAgg as getOrganizationSegmentsAgg,
} from '@crowd/data-access-layer/src/organizations/segmentsAgg'
import {
  IOrphanCleanupRun,
  IOrphanCleanupRunIncrementalUpdate,
  startOrphanCleanupRun as startCleanupRun,
  updateOrphanCleanupRun as updateCleanupRun,
} from '@crowd/data-access-layer/src/orphanSegmentAggsCleanupRuns'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'

import { svc } from '../../main'

export async function startOrphanCleanupRun(aggregateName: string): Promise<string> {
  try {
    return startCleanupRun(dbStoreQx(svc.postgres.writer), aggregateName)
  } catch (error) {
    svc.log.error(error, 'Error starting orphan cleanup run!')
    throw error
  }
}

export async function updateOrphanCleanupRun(
  runId: string,
  updates: Partial<IOrphanCleanupRun> & IOrphanCleanupRunIncrementalUpdate,
): Promise<void> {
  try {
    return updateCleanupRun(dbStoreQx(svc.postgres.writer), runId, updates)
  } catch (error) {
    svc.log.error(error, 'Error updating orphan cleanup run!')
    throw error
  }
}

export async function getOrphanMembersSegmentsAgg(batchSize: number): Promise<string[]> {
  try {
    return getMemberSegmentsAgg(dbStoreQx(svc.postgres.reader), batchSize)
  } catch (error) {
    svc.log.error(error, 'Error getting orphan memberSegmentsAgg records!')
    throw error
  }
}

export async function deleteOrphanMembersSegmentsAgg(memberId: string): Promise<void> {
  try {
    return deleteMemberSegmentsAgg(dbStoreQx(svc.postgres.writer), memberId)
  } catch (error) {
    svc.log.error(error, 'Error deleting orphan memberSegmentsAgg record!')
    throw error
  }
}

export async function getOrphanOrganizationSegmentsAgg(batchSize: number): Promise<string[]> {
  try {
    return getOrganizationSegmentsAgg(dbStoreQx(svc.postgres.reader), batchSize)
  } catch (error) {
    svc.log.error(error, 'Error getting orphan organizationSegmentsAgg records!')
    throw error
  }
}

export async function deleteOrphanOrganizationSegmentsAgg(organizationId: string): Promise<void> {
  try {
    return deleteOrganizationSegmentsAgg(dbStoreQx(svc.postgres.writer), organizationId)
  } catch (error) {
    svc.log.error(error, 'Error deleting orphan organizationSegmentsAgg record!')
    throw error
  }
}
