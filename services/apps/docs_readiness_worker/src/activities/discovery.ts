import { ApplicationFailure } from '@temporalio/client'

import { getGithubInstallationToken } from '@crowd/common_services'
import {
  findActiveProjectDocOverride,
  findEnabledRepositoriesForProject,
  findProjectForDocsDiscovery,
  findSharedDocsUrls,
  upsertProjectDocDiscovery,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { discoverDocs } from '../discovery'
import { validateDocsUrl } from '../discovery/docsValidator'
import { createAnthropicAwsDocsValidatorClient } from '../discovery/docsValidatorClient'
import { isUmbrellaWebsite } from '../discovery/sharedWebsite'
import type { DocsPickValidator } from '../discovery/strategies'
import { svc } from '../main'
import { IResolvedDocsUrl } from '../types'
import { withTimeout } from './withTimeout'

// Every individual HTTP call inside discoverDocs() carries its own AbortSignal.timeout, but
// nothing bounds the aggregate across all discovery strategies; without this, a project whose
// candidates all stall just under their own timeouts can still push the total past Temporal's
// 5-minute activity timeout, permanently occupying a worker concurrency slot instead of freeing
// it (the same failure mode fixed in scoring.ts's SCORING_TIMEOUT_MS).
const DISCOVERY_TIMEOUT_MS = 4 * 60 * 1000

// Off unless explicitly enabled; the client reads its credentials only when a check runs.
function buildDocsValidator(): DocsPickValidator | null {
  if (process.env.CROWD_DOCS_READINESS_VALIDATOR_ENABLED !== 'true') {
    return null
  }
  const client = createAnthropicAwsDocsValidatorClient()
  return (project, page) => validateDocsUrl(client, project, page)
}

export async function resolveDocsUrl(projectId: string): Promise<IResolvedDocsUrl> {
  const readerQx = pgpQx(svc.postgres.reader.connection())

  const override = await findActiveProjectDocOverride(readerQx, projectId)
  if (override) {
    if (!override.docsUrl) {
      // A no-docs override is recorded as a discovery too, so a stale discovered URL is cleared.
      await upsertProjectDocDiscovery(pgpQx(svc.postgres.writer.connection()), {
        projectId,
        docsUrl: null,
        discoveryMethod: 'override',
        confidence: 'authoritative',
        candidates: [],
      })
    }

    return {
      docsUrl: override.docsUrl,
      discoveryMethod: 'override',
      confidence: 'authoritative',
      isOverride: true,
    }
  }

  const project = await findProjectForDocsDiscovery(readerQx, projectId, {
    withWebsiteSharedWith: true,
  })
  if (!project) {
    throw ApplicationFailure.nonRetryable(`Project ${projectId} not found for docs discovery`)
  }

  const umbrella = isUmbrellaWebsite({
    name: project.name,
    slug: project.slug,
    website: project.website,
    siblings: project.websiteSharedWith,
  })

  const repos = await findEnabledRepositoriesForProject(readerQx, projectId)
  const githubToken = repos.length > 0 ? await getGithubInstallationToken() : null

  const result = await withTimeout(
    discoverDocs({
      name: project.name,
      slug: project.slug,
      website: project.website,
      websiteShared: umbrella,
      websiteSharedByFamily: project.websiteSharedWith.length > 0 && !umbrella,
      repos,
      findSharedDocsUrls: (hosts) => findSharedDocsUrls(readerQx, projectId, hosts),
      githubToken,
      serpApiKey: process.env.CROWD_DOCS_READINESS_SERP_API_KEY ?? null,
      docsValidator: buildDocsValidator(),
      log: svc.log,
    }),
    DISCOVERY_TIMEOUT_MS,
    `discoverDocs exceeded ${DISCOVERY_TIMEOUT_MS}ms for project ${projectId}`,
  )

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
