import MergeActionRepository from '@crowd/data-access-layer/src/old/apps/script_executor_worker/mergeAction.repo'
import { EntityType } from '@crowd/data-access-layer/src/old/apps/script_executor_worker/types'

import { svc } from '../main'

export async function getUnprocessedLLMApprovedSuggestions(
  batchSize: number,
  type: EntityType,
): Promise<{ primaryId: string; secondaryId: string }[]> {
  try {
    const mergeActionRepo = new MergeActionRepository(svc.postgres.reader.connection(), svc.log)
    return mergeActionRepo.getUnprocessedLLMApprovedSuggestions(batchSize, type)
  } catch (error) {
    svc.log.error(error, 'Error getting unmerged LLM approved suggestions!')
    throw error
  }
}
