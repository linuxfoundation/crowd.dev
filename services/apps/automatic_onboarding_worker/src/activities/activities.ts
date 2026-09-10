import {
  findProjectCatalogById,
  findProjectCatalogPendingOnboarding,
  markProjectCatalogOnboardingFailed,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { svc } from '../main'
import { onboardProject } from '../onboarder/onboarder'

const log = getServiceLogger()

export async function fetchProjectsPendingOnboarding(
  batchSize: number,
): Promise<IDbProjectCatalog[]> {
  const qx = pgpQx(svc.postgres.reader.connection())

  const projects = await findProjectCatalogPendingOnboarding(qx, { limit: batchSize })

  log.info({ count: projects.length, batchSize }, 'Fetched projects pending onboarding.')

  return projects
}

async function findAlreadyOnboarded(
  qx: ReturnType<typeof pgpQx>,
  projectId: string,
): Promise<IDbProjectCatalog | null> {
  const fresh = await findProjectCatalogById(qx, projectId)
  return fresh?.onboardedAt ? fresh : null
}

export async function onboardAndUpdateProject(project: IDbProjectCatalog): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const startTime = Date.now()

  // Guard: uses the writer connection to avoid replica lag missing a just-written onboardedAt.
  const fresh = await findAlreadyOnboarded(qx, project.id)
  if (fresh) {
    log.info(
      { id: project.id, repoUrl: project.repoUrl, onboardedAt: fresh.onboardedAt },
      'Project already onboarded, skipping API call.',
    )
    return
  }

  log.info({ id: project.id, repoUrl: project.repoUrl }, 'Starting onboarding.')

  const result = await onboardProject({
    id: project.id,
    repoUrl: project.repoUrl,
    repoName: project.repoName,
    projectSlug: project.projectSlug,
  })

  if (result.outcome === 'error') {
    throw new Error(result.error ?? 'Unknown onboarding error')
  }

  await updateProjectCatalog(qx, project.id, {
    action: 'onboarded',
    onboardedAt: new Date().toISOString(),
    onboardingError: null,
  })

  const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1)

  log.info(
    { id: project.id, repoUrl: project.repoUrl, segmentId: result.segmentId, elapsedSeconds },
    'Onboarding complete.',
  )
}

export async function markProjectOnboardingFailed(
  projectId: string,
  reason: string,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  const updatedRows = await markProjectCatalogOnboardingFailed(qx, projectId, reason)

  if (updatedRows === 0) {
    log.info(
      { id: projectId },
      'Project was already onboarded or no longer pending, not marking as error.',
    )
    return
  }

  log.error({ id: projectId, reason }, 'Onboarding permanently failed, marked as error.')
}
