import { ApplicationFailure } from '@temporalio/client'

import { getGithubInstallationToken } from '@crowd/common_services'
import {
  findActiveProjectDocOverride,
  findEnabledRepositoriesForProject,
  findProjectForDocsDiscovery,
  upsertProjectDocDiscovery,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { discoverDocs } from '../discovery'
import { svc } from '../main'
import { IResolvedDocsUrl } from '../types'

export async function resolveDocsUrl(projectId: string): Promise<IResolvedDocsUrl> {
  const readerQx = pgpQx(svc.postgres.reader.connection())

  const override = await findActiveProjectDocOverride(readerQx, projectId)
  if (override) {
    return {
      docsUrl: override.docsUrl,
      discoveryMethod: 'override',
      confidence: 'authoritative',
      isOverride: true,
    }
  }

  const project = await findProjectForDocsDiscovery(readerQx, projectId)
  if (!project) {
    throw ApplicationFailure.nonRetryable(`Project ${projectId} not found for docs discovery`)
  }

  const repos = await findEnabledRepositoriesForProject(readerQx, projectId)
  const githubToken = repos.length > 0 ? await getGithubInstallationToken() : null

  const result = await discoverDocs({
    name: project.name,
    website: project.website,
    repos: repos.map((r) => r.url),
    githubToken,
    serpApiKey: process.env.CROWD_DOCS_READINESS_SERP_API_KEY ?? null,
  })

  const writerQx = pgpQx(svc.postgres.writer.connection())
  await upsertProjectDocDiscovery(writerQx, {
    projectId,
    docsUrl: result.docsUrl,
    discoveryMethod: result.discoveryMethod,
    confidence: result.confidence,
    candidates: result.allCandidates,
  })

  return {
    docsUrl: result.docsUrl,
    discoveryMethod: result.discoveryMethod,
    confidence: result.confidence,
    isOverride: false,
  }
}
