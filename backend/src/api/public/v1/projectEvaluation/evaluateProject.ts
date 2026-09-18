import {
  IPublicRepoMetrics,
  IPublicRepoReadme,
  fetchPublicRepoMetrics,
  fetchPublicRepoReadme,
  getErrorMessage,
  getGithubToken,
  parseLlmJson,
} from '@crowd/common'
import { IBedrockClientCredentials, LlmService } from '@crowd/common_services'
import { QueryExecutor } from '@crowd/data-access-layer'
import {
  IProjectEvaluationRequest,
  IProjectEvaluationResponse,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { Logger } from '@crowd/logging'
import { LlmQueryType } from '@crowd/types'

interface IProjectEvaluationDecision {
  onboard: boolean
  non_onboard_reason?: string
}

function buildPrompt(
  input: IProjectEvaluationRequest,
  metrics: IPublicRepoMetrics,
  readme: IPublicRepoReadme | null,
): string {
  return `You decide whether an open-source GitHub repository should be onboarded onto a project catalog.

Repository: ${input.repoUrl}
Description: ${metrics.description ?? 'none'}
Primary language: ${metrics.primaryLanguage ?? 'unknown'}
Stars: ${metrics.stars}, forks: ${metrics.forks}
Open/closed issues: ${metrics.openIssues}/${metrics.closedIssues}
Open/closed pull requests: ${metrics.openPullRequests}/${metrics.closedPullRequests}
Archived: ${metrics.isArchived}, fork: ${metrics.isFork}

README (truncated to ${readme?.truncated ? 'first N characters' : 'full content'}):
${readme?.content ?? '(no README found)'}

Evaluate the repository against these criteria, in order. Stop and answer as soon as one matches:

1. Documentation repo: the repository is a documentation site, an SDK, a website, a recipe collection, or a notes repo with no core project code.
2. Linux kernel fork: the repository is a fork of, or based on, the Linux kernel (look for kernel-specific terms like "vmlinux", "CONFIG_", "arch/x86", "drivers/", or explicit mentions of being a Linux kernel fork).
3. Not mainly run on GitHub: the closed pull request and issue counts are near zero relative to the project's age, suggesting the project is developed elsewhere and only mirrored here.

If none of the criteria match, the repository should be onboarded.

Respond with ONLY a JSON object, no other text, matching exactly one of these two shapes:
- If onboarding: {"onboard": true}
- If not onboarding: {"onboard": false, "non_onboard_reason": "<one of: 'project is a documentation repo, an SDK, a website, a recipe, etc', 'project is a fork of the linux kernel with some additions', 'project is not mainly run on GitHub'>"}`
}

function errorResult(err: unknown): IProjectEvaluationResponse {
  return {
    outcome: 'unsure',
    evaluationResult: 'error',
    evaluationReason: getErrorMessage(err),
    metrics: null,
  }
}

export async function evaluateProject(
  input: IProjectEvaluationRequest,
  qx: QueryExecutor,
  bedrockCredentials: IBedrockClientCredentials,
  log: Logger,
): Promise<IProjectEvaluationResponse> {
  let token: string
  try {
    token = getGithubToken()
  } catch (err) {
    return errorResult(err)
  }

  let metrics: IPublicRepoMetrics
  let readme: IPublicRepoReadme | null
  try {
    const results = await Promise.all([
      fetchPublicRepoMetrics(input.repoUrl, token),
      fetchPublicRepoReadme(input.repoUrl, token),
    ])
    metrics = results[0]
    readme = results[1]
  } catch (err) {
    return errorResult(err)
  }

  const llmService = new LlmService(qx, bedrockCredentials, log)

  let response: Awaited<ReturnType<LlmService['queryLlm']>>
  try {
    response = await llmService.queryLlm(
      LlmQueryType.PROJECT_EVALUATION,
      buildPrompt(input, metrics, readme),
      input.id,
    )
  } catch (err) {
    return errorResult(err)
  }

  if (!response) {
    return errorResult(
      new Error('LLM query returned no response — check CROWD_LLM_ENABLED and Bedrock credentials'),
    )
  }

  let decision: IProjectEvaluationDecision
  try {
    decision = parseLlmJson<IProjectEvaluationDecision>(response.answer)
  } catch (err) {
    return errorResult(err)
  }

  if (typeof decision?.onboard !== 'boolean') {
    return errorResult(new Error(`Unexpected LLM decision shape: ${JSON.stringify(decision)}`))
  }

  return {
    outcome: decision.onboard ? 'onboard' : 'skip',
    evaluationResult: String(decision.onboard),
    evaluationReason: decision.non_onboard_reason ?? null,
    metrics: {
      model: response.model,
      inputTokens: response.inputTokenCount,
      outputTokens: response.outputTokenCount,
      seconds: response.responseTimeSeconds,
    },
  }
}
