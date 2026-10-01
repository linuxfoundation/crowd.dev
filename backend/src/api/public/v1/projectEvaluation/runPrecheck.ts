import { canonicalizeRepoUrl } from '@crowd/common'
import {
  computeExclusivelyLfOwners,
  findGithubOwnersWithLfProjects,
  findGithubOwnersWithNonLfRepos,
  findRepoUrlsInCdp,
  resolvePrecheckSkipReason,
} from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { IProjectEvaluationResponse } from './types'

export async function runPrecheck(qx: QueryExecutor, repoUrl: string): Promise<string | null> {
  const canonical = canonicalizeRepoUrl(repoUrl)
  const githubCanonical = canonical?.isGithub ? canonical : null

  const [reposInCdp, lfOwners, nonLfOwners] = await Promise.all([
    findRepoUrlsInCdp(qx, githubCanonical ? [githubCanonical.url] : []),
    findGithubOwnersWithLfProjects(qx, githubCanonical ? [githubCanonical.owner] : []),
    findGithubOwnersWithNonLfRepos(qx, githubCanonical ? [githubCanonical.owner] : []),
  ])

  return resolvePrecheckSkipReason(canonical, {
    reposInCdp,
    exclusivelyLfOwners: computeExclusivelyLfOwners(lfOwners, nonLfOwners),
  })
}

export function precheckSkipResult(reason: string): IProjectEvaluationResponse {
  return {
    outcome: 'skip',
    evaluationResult: 'false',
    evaluationReason: reason,
    metrics: null,
  }
}
