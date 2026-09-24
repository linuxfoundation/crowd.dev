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
  hasIssuesEnabled: true,
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
    const nonOnboardReason = 'project is a documentation repo, an SDK, a website, a recipe, etc'
    queryLlm.mockResolvedValue({
      answer: `{"onboard": false, "non_onboard_reason": "${nonOnboardReason}"}`,
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockReturnValue({
      onboard: false,
      non_onboard_reason: nonOnboardReason,
    })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('skip')
    expect(result.evaluationReason).toBe(nonOnboardReason)
  })

  it('returns an unsure/error result when the non-onboard reason is not one of the allowed values', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue({
      answer: '{"onboard": false, "non_onboard_reason": "I just felt like it"}',
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockReturnValue({ onboard: false, non_onboard_reason: 'I just felt like it' })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationResult).toBe('error')
  })

  it('returns an unsure/error result when onboard is false without a reason', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue({
      answer: '{"onboard": false}',
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockReturnValue({ onboard: false })

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result.outcome).toBe('unsure')
    expect(result.evaluationResult).toBe('error')
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

  it('skips deterministically when closed activity and stars are both near zero', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue({
      ...metrics,
      stars: 0,
      forks: 0,
      closedIssues: 0,
      closedPullRequests: 0,
    })
    fetchPublicRepoReadme.mockResolvedValue(readme)

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result).toEqual({
      outcome: 'skip',
      evaluationResult: 'false',
      evaluationReason: 'project has insufficient GitHub activity to evaluate',
      metrics: null,
    })
    expect(queryLlm).not.toHaveBeenCalled()
  })

  it('does not skip deterministically when stars are high even with low closed activity', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue({
      ...metrics,
      stars: 100,
      closedIssues: 0,
      closedPullRequests: 1,
    })
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

    expect(result.outcome).toBe('onboard')
    expect(queryLlm).toHaveBeenCalledOnce()
  })

  it('tells the LLM when GitHub Issues is disabled instead of reading it as low activity', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue({
      ...metrics,
      stars: 10,
      closedIssues: 0,
      hasIssuesEnabled: false,
      closedPullRequests: 17356,
    })
    fetchPublicRepoReadme.mockResolvedValue(readme)
    queryLlm.mockResolvedValue({
      answer: '{"onboard": true}',
      model: 'test-model',
      inputTokenCount: 10,
      outputTokenCount: 5,
      responseTimeSeconds: 1.2,
    })
    parseLlmJson.mockReturnValue({ onboard: true })

    await evaluateProject(input, qx, bedrockCredentials, log)

    const prompt = queryLlm.mock.calls[0][1] as string
    expect(prompt).toContain('GitHub Issues is disabled on this repo')
  })

  it('skips deterministically when issues are disabled and closed PRs are also near zero', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue({
      ...metrics,
      stars: 0,
      closedIssues: 5,
      hasIssuesEnabled: false,
      closedPullRequests: 0,
    })
    fetchPublicRepoReadme.mockResolvedValue(readme)

    const result = await evaluateProject(input, qx, bedrockCredentials, log)

    expect(result).toEqual({
      outcome: 'skip',
      evaluationResult: 'false',
      evaluationReason: 'project has insufficient GitHub activity to evaluate',
      metrics: null,
    })
    expect(queryLlm).not.toHaveBeenCalled()
  })

  it('does not reserve a daily LLM call when the insufficient-activity shortcut fires', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue({
      ...metrics,
      stars: 0,
      forks: 0,
      closedIssues: 0,
      closedPullRequests: 0,
    })
    fetchPublicRepoReadme.mockResolvedValue(readme)
    const reserveLlmCall = vi.fn()

    await evaluateProject(input, qx, bedrockCredentials, log, reserveLlmCall)

    expect(reserveLlmCall).not.toHaveBeenCalled()
  })

  it('reserves exactly one daily LLM call before querying the LLM', async () => {
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
    const reserveLlmCall = vi.fn()

    await evaluateProject(input, qx, bedrockCredentials, log, reserveLlmCall)

    expect(reserveLlmCall).toHaveBeenCalledOnce()
  })

  it('propagates a thrown reservation error instead of returning an errorResult', async () => {
    getGithubToken.mockReturnValue('token')
    fetchPublicRepoMetrics.mockResolvedValue(metrics)
    fetchPublicRepoReadme.mockResolvedValue(readme)
    const reserveLlmCall = vi.fn(() => {
      throw new Error('Daily evaluation limit reached')
    })

    await expect(
      evaluateProject(input, qx, bedrockCredentials, log, reserveLlmCall),
    ).rejects.toThrow('Daily evaluation limit reached')
    expect(queryLlm).not.toHaveBeenCalled()
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
