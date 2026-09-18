import { beforeEach, describe, expect, it, vi } from 'vitest'

import { IPublicRepoMetrics, IPublicRepoReadme } from '@crowd/common'
import { IProjectEvaluationRequest } from '@crowd/data-access-layer/src/project-catalog/types'
import { Logger } from '@crowd/logging'

import { evaluateProject } from './evaluateProject'

const fetchPublicRepoMetrics = vi.fn()
const fetchPublicRepoReadme = vi.fn()
const getGithubToken = vi.fn()
const parseLlmJson = vi.fn()
const queryLlm = vi.fn()

vi.mock('@crowd/common', () => ({
  fetchPublicRepoMetrics: (...args: unknown[]) => fetchPublicRepoMetrics(...args),
  fetchPublicRepoReadme: (...args: unknown[]) => fetchPublicRepoReadme(...args),
  getGithubToken: (...args: unknown[]) => getGithubToken(...args),
  parseLlmJson: (...args: unknown[]) => parseLlmJson(...args),
  getErrorMessage: (err: unknown) => (err instanceof Error ? err.message : String(err)),
}))

vi.mock('@crowd/common_services', () => ({
  LlmService: class {
    queryLlm(...args: unknown[]) {
      return queryLlm(...args)
    }
  },
}))

const input: IProjectEvaluationRequest = {
  id: 'catalog-1',
  repoUrl: 'https://github.com/foo/bar',
  repoName: 'bar',
  projectSlug: 'foo',
  lfCriticalityScore: null,
  source: null,
}

const metrics: IPublicRepoMetrics = {
  description: 'a repo',
  primaryLanguage: 'TypeScript',
  stars: 10,
  forks: 2,
  openIssues: 1,
  closedIssues: 5,
  openPullRequests: 1,
  closedPullRequests: 5,
  pushedAt: '2024-01-01',
  createdAt: '2020-01-01',
  isArchived: false,
  isFork: false,
}

const readme: IPublicRepoReadme = { content: 'readme content', truncated: false }

const qx = {} as never
const bedrockCredentials = { accessKeyId: 'key', secretAccessKey: 'secret' }
const log = { warn: vi.fn(), error: vi.fn() } as unknown as Logger

describe('evaluateProject', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('returns onboard when the LLM decides to onboard', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue({
      answer: '{"onboard": true}',
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockReturnValue({ onboard: true })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result).toEqual({
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: { model: 'test-model', inputTokens: 10, outputTokens: 5, seconds: 1.2 },
    })
  })

  it('returns skip with a reason when the LLM decides not to onboard', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue({
      answer: '{"onboard": false, "non_onboard_reason": "project is a documentation repo"}',
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockReturnValue({
      onboard: false,
      non_onboard_reason: 'project is a documentation repo',
    })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('skip')
    expect(result.evaluationReason).toBe('project is a documentation repo')
  })

  it('returns an unsure/error result when the GitHub token is missing', async () => {
    getGithubToken.mockImplementation(() => {
      throw new Error('Missing CROWD_PROJECT_EVALUATION_GITHUB_TOKEN configuration')
    })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result).toEqual({
      outcome: 'unsure',
      evaluationResult: 'error',
      evaluationReason: 'Missing CROWD_PROJECT_EVALUATION_GITHUB_TOKEN configuration',
      metrics: null,
    })
    expect(fetchPublicRepoMetrics).not.toHaveBeenCalled()
  })

  it('returns an unsure/error result when fetching GitHub data fails', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockRejectedValue(new Error('rate limited'))
    fetchPublicRepoReadme.mockResolvedValue(readme)

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toBe('rate limited')
  })

  it('returns an unsure/error result when the LLM query throws', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockRejectedValue(new Error('bedrock unavailable'))

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toBe('bedrock unavailable')
  })

  it('returns an unsure/error result when the LLM returns no response', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue(undefined)

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toContain('CROWD_LLM_ENABLED')
  })

  it('returns an unsure/error result when the LLM answer is not parseable JSON', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue({
      answer: 'not json',
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockImplementation(() => {
      throw new SyntaxError('Unexpected token')
    })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationReason).toBe('Unexpected token')
  })
})
