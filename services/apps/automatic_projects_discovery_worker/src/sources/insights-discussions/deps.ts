import { Context } from '@temporalio/activity'

import { isPublicGithubRepo } from '@crowd/common'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { OnboardingRequestLlm, parseOnboardingRequest } from '@crowd/project-onboarding'
import { createOnboardingRequestLlm } from '@crowd/project-onboarding/src/requestLlm'

import { svc } from '../../main'
import { IDiscussionSourceDeps, githubToken } from './source'

const GITHUB_REPO_CHECK_TIMEOUT_MS = 10_000

export function workerDiscussionSourceDeps(): IDiscussionSourceDeps {
  let llm: OnboardingRequestLlm | undefined
  const queryLlm = () =>
    (llm ??= createOnboardingRequestLlm(pgpQx(svc.postgres.writer.connection())))

  return {
    parseRequest: (text) => parseOnboardingRequest(text, queryLlm()),
    lookupRepository: (repoUrl) =>
      isPublicGithubRepo(repoUrl, githubToken(), AbortSignal.timeout(GITHUB_REPO_CHECK_TIMEOUT_MS)),
    heartbeat: (details) => Context.current().heartbeat(details),
  }
}
