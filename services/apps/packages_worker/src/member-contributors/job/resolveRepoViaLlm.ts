import { ApplicationFailure } from '@temporalio/activity'

import {
  recordMemberContributorJobInferredRepo,
  recordMemberContributorJobLlmUsage,
} from '@crowd/data-access-layer/src/osspckgs/memberContributorJobs'
import { LLM_MODEL_REGION_MAP } from '@crowd/types'

import { getBedrockCredentials, getGithubAppConfig } from '../../config'
import { getPackagesDb } from '../../db'
import { invokeBedrockModel } from '../../llm/bedrockClient'
import { githubApiGet } from '../../security-contacts/githubToken'
import { canonicalGithubUrl } from './canonicalGithubUrl'
import {
  REPO_INFERENCE_MODEL_ID,
  RepoCandidate,
  TRUNCATED_ANSWER,
  inferRepositoryCandidates,
} from './llmRepoInference'

export const MIN_REPO_CONFIDENCE = 0.7
const LLM_TIMEOUT_MS = 60_000
const GITHUB_TIMEOUT_MS = 15_000

export interface ResolveRepoViaLlmInput {
  jobId: string
  purl: string
}

export type ResolveRepoViaLlmResult =
  | { outcome: 'resolved'; repoUrl: string; confidence: number }
  | { outcome: 'unresolvable'; reason: string; repoUrl: string | null }

type Unresolvable = Extract<ResolveRepoViaLlmResult, { outcome: 'unresolvable' }>

function unresolvable(reason: string, repoUrl: string | null = null): Unresolvable {
  return { outcome: 'unresolvable', reason, repoUrl }
}

async function repoExistsOnGithub(owner: string, name: string): Promise<boolean> {
  const result = await githubApiGet(`/repos/${owner}/${name}`, GITHUB_TIMEOUT_MS)
  return result.status === 200
}

async function pickExistingCandidate(
  candidates: RepoCandidate[],
): Promise<ResolveRepoViaLlmResult> {
  const confident = candidates.filter((candidate) => candidate.confidence >= MIN_REPO_CONFIDENCE)
  if (confident.length === 0) {
    const highest = Math.max(...candidates.map((candidate) => candidate.confidence))
    return unresolvable(`LLM confidence ${highest} below threshold ${MIN_REPO_CONFIDENCE}`)
  }
  let last: Unresolvable = unresolvable('LLM returned no repository')
  for (const candidate of confident) {
    const parsed = canonicalGithubUrl(candidate.repositoryUrl)
    if (parsed.kind === 'other') {
      last = unresolvable(`Repository host not supported: ${parsed.host}`, candidate.repositoryUrl)
      continue
    }
    if (parsed.kind === 'invalid') {
      last = unresolvable(
        `Repository URL not recognised: ${candidate.repositoryUrl}`,
        candidate.repositoryUrl,
      )
      continue
    }
    if (await repoExistsOnGithub(parsed.owner, parsed.name)) {
      return { outcome: 'resolved', repoUrl: parsed.url, confidence: candidate.confidence }
    }
    last = unresolvable(`Repository not found on GitHub: ${parsed.url}`, parsed.url)
  }
  return last
}

export async function resolveRepoViaLlm(
  input: ResolveRepoViaLlmInput,
): Promise<ResolveRepoViaLlmResult> {
  const credentials = getBedrockCredentials()
  if (!credentials) {
    throw ApplicationFailure.nonRetryable('Bedrock credentials are not configured')
  }
  getGithubAppConfig()
  const invoke = invokeBedrockModel(
    LLM_MODEL_REGION_MAP[REPO_INFERENCE_MODEL_ID],
    credentials,
    LLM_TIMEOUT_MS,
  )
  const inference = await inferRepositoryCandidates(input.purl, invoke)
  const pkgsQx = await getPackagesDb()
  await recordMemberContributorJobLlmUsage(pkgsQx, input.jobId, inference.usage)

  if (inference.status === 'truncated') {
    throw new Error(TRUNCATED_ANSWER)
  }
  if (inference.status === 'unparseable') {
    return unresolvable(inference.reason)
  }
  if (inference.candidates.length === 0) {
    return unresolvable('LLM returned no repository')
  }
  const result = await pickExistingCandidate(inference.candidates)
  if (result.outcome === 'resolved') {
    await recordMemberContributorJobInferredRepo(pkgsQx, input.jobId, {
      repoUrl: result.repoUrl,
      confidence: result.confidence,
    })
  }
  return result
}
