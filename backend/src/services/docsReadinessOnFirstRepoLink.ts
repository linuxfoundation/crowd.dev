import { QueryExecutor } from '@crowd/data-access-layer'
import { findInsightsProjectIdsWithRepositories } from '@crowd/data-access-layer/src/repositories'

import { CollectionService } from './collectionService'
import { IServiceOptions } from './IServiceOptions'

export async function findProjectIdsGettingFirstRepository(
  qx: QueryExecutor,
  repositories: { insightsProjectId?: string }[],
): Promise<string[]> {
  const projectIds = [...new Set(repositories.map((repo) => repo.insightsProjectId))].filter(
    (id): id is string => Boolean(id),
  )
  const projectIdsWithRepositories = new Set(
    await findInsightsProjectIdsWithRepositories(qx, projectIds),
  )
  return projectIds.filter((id) => !projectIdsWithRepositories.has(id))
}

export async function startDocsReadinessForProjects(
  options: IServiceOptions,
  projectIds: string[],
): Promise<void> {
  const collectionService = new CollectionService(options)

  for (const projectId of projectIds) {
    try {
      await collectionService.startDocsReadinessWorkflow(projectId)
    } catch (err) {
      options.log.error(
        err,
        { projectId },
        'Failed to start docs readiness workflow after first repository link',
      )
    }
  }
}
