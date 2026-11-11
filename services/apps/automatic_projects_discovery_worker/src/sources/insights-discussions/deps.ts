import { Context } from '@temporalio/activity'

import { fetchPublicOrgRepos, isPublicGithubRepo } from '@crowd/common'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  OnboardingRequestLlm,
  parseOnboardingRequest,
  refreshDiscussionStatusComment,
} from '@crowd/project-onboarding'
import { createOnboardingRequestLlm } from '@crowd/project-onboarding/src/requestLlm'

import { svc } from '../../main'
import { IDiscussionSourceDeps, githubToken } from './source'

const GITHUB_LOOKUP_TIMEOUT_MS = 10_000

export function workerDiscussionSourceDeps(): IDiscussionSourceDeps {
  let llm: OnboardingRequestLlm | undefined
  const queryLlm = () =>
    (llm ??= createOnboardingRequestLlm(pgpQx(svc.postgres.writer.connection())))

  return {
    parseRequest: (text) => parseOnboardingRequest(text, queryLlm()),
    lookupRepository: (repoUrl) =>
      isPublicGithubRepo(repoUrl, githubToken(), AbortSignal.timeout(GITHUB_LOOKUP_TIMEOUT_MS)),
    lookupOrganization: (owner, limit) =>
      fetchPublicOrgRepos(
        owner,
        githubToken(),
        limit,
        AbortSignal.timeout(GITHUB_LOOKUP_TIMEOUT_MS),
      ),
    noteOrganizationFailures: async (discussionUrl, notes) => {
      await refreshDiscussionStatusComment(
        pgpQx(svc.postgres.writer.connection()),
        { sourceUrl: discussionUrl },
        null,
        { organizationNotes: notes },
      )
    },
    heartbeat: (details) => Context.current().heartbeat(details),
  }
}
