import { findProjectCatalogPendingEvaluation, updateProjectCatalog } from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { evaluateProject } from '../evaluator/evaluator'
import { svc } from '../main'

const log = getServiceLogger()

export async function fetchPendingProjects(batchSize: number): Promise<IDbProjectCatalog[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  
  const projects = await findProjectCatalogPendingEvaluation(qx, { limit: batchSize })

  log.info({ count: projects.length, batchSize }, 'Fetched projects pending evaluation.')

  return projects
}

export async function evaluateAndUpdateProject(project: IDbProjectCatalog): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  log.info({ id: project.id, repoUrl: project.repoUrl }, 'Starting evaluation.')

  const result = await evaluateProject({
    id: project.id,
    repoUrl: project.repoUrl,
    repoName: project.repoName,
    projectSlug: project.projectSlug,
    lfCriticalityScore: project.lfCriticalityScore,
    source: project.source,
  })

  await updateProjectCatalog(qx, project.id, {
    action: result.outcome,
    evaluatedAt: new Date().toISOString(),
  })

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  log.info(
    {
      id: project.id,
      repoUrl: project.repoUrl,
      outcome: result.outcome,
      reason: result.reason,
      elapsedSeconds,
    },
    'Evaluation complete.',
  )
}
