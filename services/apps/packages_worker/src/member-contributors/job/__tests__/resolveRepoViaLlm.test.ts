import { beforeEach, describe, expect, it, vi } from 'vitest'

import * as jobsDal from '@crowd/data-access-layer/src/osspckgs/memberContributorJobs'

import { getBedrockCredentials, getGithubAppConfig } from '../../../config'
import { githubApiGet } from '../../../security-contacts/githubToken'
import { inferRepositoryCandidates } from '../llmRepoInference'
import { resolveRepoViaLlm } from '../resolveRepoViaLlm'

const qx = vi.hoisted(() => ({}))

vi.mock('../../../db', () => ({ getPackagesDb: vi.fn().mockResolvedValue(qx) }))
vi.mock('../../../config', () => ({
  getBedrockCredentials: vi.fn(),
  getGithubAppConfig: vi.fn(),
}))
vi.mock('../../../llm/bedrockClient', () => ({ invokeBedrockModel: vi.fn(() => vi.fn()) }))
vi.mock('../../../security-contacts/githubToken', () => ({ githubApiGet: vi.fn() }))
vi.mock('../llmRepoInference', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../llmRepoInference')>()),
  inferRepositoryCandidates: vi.fn(),
}))
vi.mock('@crowd/data-access-layer/src/osspckgs/memberContributorJobs', () => ({
  recordMemberContributorJobInferredRepo: vi.fn(),
  recordMemberContributorJobLlmUsage: vi.fn(),
}))

const usage = { inputTokens: 300, outputTokens: 80, costUsd: 0.0014 }
const input = { jobId: 'job-1', purl: 'pkg:hackage/aeson' }
const recordRepo = vi.mocked(jobsDal.recordMemberContributorJobInferredRepo)
const recordUsage = vi.mocked(jobsDal.recordMemberContributorJobLlmUsage)
const found = { status: 200, text: '{}' }
const absent = { status: 404, text: null }

type Inference = Awaited<ReturnType<typeof inferRepositoryCandidates>>
type GithubResult = Awaited<ReturnType<typeof githubApiGet>>

function answered(candidates: Array<{ repositoryUrl: string; confidence: number }>): Inference {
  return { status: 'answered', candidates, usage }
}

function githubCheck(path: string) {
  return [path, 15000]
}

function expectUsageRecordedOnceBeforeGithub() {
  expect(recordUsage.mock.calls).toEqual([[qx, 'job-1', usage]])
  const githubCalls = vi.mocked(githubApiGet).mock.invocationCallOrder
  if (githubCalls.length > 0) {
    expect(recordUsage.mock.invocationCallOrder[0]).toBeLessThan(githubCalls[0])
  }
}

async function resolveWith(inference: Inference, githubResults: GithubResult[]) {
  vi.mocked(inferRepositoryCandidates).mockResolvedValue(inference)
  for (const githubResult of githubResults) {
    vi.mocked(githubApiGet).mockResolvedValueOnce(githubResult)
  }
  const result = await resolveRepoViaLlm(input)
  expectUsageRecordedOnceBeforeGithub()
  return {
    result,
    githubCalls: vi.mocked(githubApiGet).mock.calls,
    recordedRepos: recordRepo.mock.calls,
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(getBedrockCredentials).mockReturnValue({ accessKeyId: 'k', secretAccessKey: 's' })
  vi.mocked(getGithubAppConfig).mockReturnValue(
    {} as unknown as ReturnType<typeof getGithubAppConfig>,
  )
  vi.mocked(githubApiGet).mockResolvedValue(found)
})

describe('resolveRepoViaLlm', () => {
  it.each([
    {
      name: 'canonicalises the first existing candidate',
      inference: answered([
        { repositoryUrl: 'https://github.com/Haskell/aeson.git', confidence: 0.95 },
      ]),
      github: [found],
      checked: [githubCheck('/repos/haskell/aeson')],
      repoUrl: 'https://github.com/haskell/aeson',
      confidence: 0.95,
    },
    {
      name: 'skips an absent candidate for the next existing one',
      inference: answered([
        { repositoryUrl: 'https://github.com/bos/aeson', confidence: 0.9 },
        { repositoryUrl: 'https://github.com/haskell/aeson', confidence: 0.8 },
      ]),
      github: [absent, found],
      checked: [githubCheck('/repos/bos/aeson'), githubCheck('/repos/haskell/aeson')],
      repoUrl: 'https://github.com/haskell/aeson',
      confidence: 0.8,
    },
  ])(
    '$name and records it on the job',
    async ({ inference, github, checked, repoUrl, confidence }) => {
      await expect(resolveWith(inference, github)).resolves.toEqual({
        result: { outcome: 'resolved', repoUrl, confidence },
        githubCalls: checked,
        recordedRepos: [[qx, 'job-1', { repoUrl, confidence }]],
      })
    },
  )

  it.each([
    {
      name: 'every candidate is absent, keeping the last attempted url',
      inference: answered([
        { repositoryUrl: 'https://github.com/a/one', confidence: 0.9 },
        { repositoryUrl: 'https://github.com/a/two', confidence: 0.8 },
      ]),
      github: [absent, absent],
      checked: [githubCheck('/repos/a/one'), githubCheck('/repos/a/two')],
      reason: 'Repository not found on GitHub: https://github.com/a/two',
      repoUrl: 'https://github.com/a/two',
    },
    {
      name: 'the only candidate is below the confidence threshold',
      inference: answered([{ repositoryUrl: 'https://github.com/a/b', confidence: 0.4 }]),
      github: [],
      checked: [],
      reason: 'LLM confidence 0.4 below threshold 0.7',
      repoUrl: null,
    },
    {
      name: 'all candidates are below the threshold, quoting the highest confidence',
      inference: answered([
        { repositoryUrl: 'https://github.com/a/b', confidence: 0.4 },
        { repositoryUrl: 'https://github.com/a/c', confidence: 0.6 },
      ]),
      github: [],
      checked: [],
      reason: 'LLM confidence 0.6 below threshold 0.7',
      repoUrl: null,
    },
    {
      name: 'the confident candidate is absent and lower-ranked ones are below the threshold',
      inference: answered([
        { repositoryUrl: 'https://github.com/a/one', confidence: 0.9 },
        { repositoryUrl: 'https://github.com/a/two', confidence: 0.2 },
      ]),
      github: [absent],
      checked: [githubCheck('/repos/a/one')],
      reason: 'Repository not found on GitHub: https://github.com/a/one',
      repoUrl: 'https://github.com/a/one',
    },
    {
      name: 'the candidate host is not GitHub',
      inference: answered([{ repositoryUrl: 'https://gitlab.com/a/b', confidence: 0.9 }]),
      github: [],
      checked: [],
      reason: 'Repository host not supported: gitlab.com',
      repoUrl: 'https://gitlab.com/a/b',
    },
    {
      name: 'the candidate url is not recognised',
      inference: answered([{ repositoryUrl: 'not a url', confidence: 0.9 }]),
      github: [],
      checked: [],
      reason: 'Repository URL not recognised: not a url',
      repoUrl: 'not a url',
    },
    {
      name: 'the LLM returns no candidate',
      inference: answered([]),
      github: [],
      checked: [],
      reason: 'LLM returned no repository',
      repoUrl: null,
    },
    {
      name: 'the LLM answer is unparseable',
      inference: { status: 'unparseable', reason: 'LLM answer was not valid JSON', usage },
      github: [],
      checked: [],
      reason: 'LLM answer was not valid JSON',
      repoUrl: null,
    },
  ] satisfies Array<{
    name: string
    inference: Inference
    github: GithubResult[]
    checked: Array<Array<string | number>>
    reason: string
    repoUrl: string | null
  }>)('is unresolvable when $name', async ({ inference, github, checked, reason, repoUrl }) => {
    await expect(resolveWith(inference, github)).resolves.toEqual({
      result: { outcome: 'unresolvable', reason, repoUrl },
      githubCalls: checked,
      recordedRepos: [],
    })
  })

  it.each([
    {
      name: 'retryably when the GitHub existence check fails, after recording usage',
      arrange: () => {
        vi.mocked(inferRepositoryCandidates).mockResolvedValue(
          answered([{ repositoryUrl: 'https://github.com/a/b', confidence: 0.9 }]),
        )
        vi.mocked(githubApiGet).mockRejectedValue(new Error('HTTP 502'))
      },
      expected: {
        message: 'HTTP 502',
        nonRetryable: false,
        llmCalls: 1,
        usageCalls: [[qx, 'job-1', usage]],
        githubCalls: 1,
      },
    },
    {
      name: 'retryably when the LLM answer is truncated, after recording usage',
      arrange: () => {
        vi.mocked(inferRepositoryCandidates).mockResolvedValue({ status: 'truncated', usage })
      },
      expected: {
        message: 'LLM answer truncated at max_tokens',
        nonRetryable: false,
        llmCalls: 1,
        usageCalls: [[qx, 'job-1', usage]],
        githubCalls: 0,
      },
    },
    {
      name: 'before calling the LLM when the GitHub App config is missing',
      arrange: () => {
        vi.mocked(getGithubAppConfig).mockImplementation(() => {
          throw new Error('Missing required environment variable: CROWD_GITHUB_PRIVATE_KEY')
        })
      },
      expected: {
        message: 'Missing required environment variable: CROWD_GITHUB_PRIVATE_KEY',
        nonRetryable: false,
        llmCalls: 0,
        usageCalls: [],
        githubCalls: 0,
      },
    },
    {
      name: 'without retry and before calling the LLM when Bedrock credentials are missing',
      arrange: () => {
        vi.mocked(getBedrockCredentials).mockReturnValue(null)
      },
      expected: {
        message: 'Bedrock credentials are not configured',
        nonRetryable: true,
        llmCalls: 0,
        usageCalls: [],
        githubCalls: 0,
      },
    },
  ])('fails $name', async ({ arrange, expected }) => {
    arrange()
    const error = (await resolveRepoViaLlm(input).catch((e: unknown) => e)) as Error & {
      nonRetryable?: boolean
    }
    expect({
      message: error.message,
      nonRetryable: error.nonRetryable === true,
      llmCalls: vi.mocked(inferRepositoryCandidates).mock.calls.length,
      usageCalls: recordUsage.mock.calls,
      githubCalls: vi.mocked(githubApiGet).mock.calls.length,
    }).toEqual(expected)
    expect(recordRepo).not.toHaveBeenCalled()
  })
})
