import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('axios', () => ({
  default: { post: vi.fn(async () => undefined) },
}))

vi.mock('../../database/sequelizeQueryExecutor', () => ({
  optionsQx: vi.fn(() => ({})),
}))

vi.mock('@crowd/data-access-layer', () => ({
  deriveProjectIdentityFromRepoUrl: vi.fn(() => ({ projectSlug: 'foo/bar', repoName: 'bar' })),
  upsertProjectCatalogManualAction: vi.fn(),
  finalizeProjectCatalogEvaluation: vi.fn(),
  findProjectCatalogById: vi.fn(async () => ({ onboardedAt: null })),
  updateProjectCatalog: vi.fn(async () => ({})),
}))

vi.mock('../../api/public/v1/projectEvaluation/evaluateProject', () => ({
  evaluateProject: vi.fn(),
}))

vi.mock('@crowd/project-onboarding', () => ({
  onboardProject: vi.fn(),
}))

import {
  deriveProjectIdentityFromRepoUrl,
  finalizeProjectCatalogEvaluation,
  findProjectCatalogById,
  updateProjectCatalog,
  upsertProjectCatalogManualAction,
} from '@crowd/data-access-layer'
import { onboardProject } from '@crowd/project-onboarding'

import { evaluateProject } from '../../api/public/v1/projectEvaluation/evaluateProject'
import { runOnboardProjectCommand } from './onboardProjectCommand'

const catalogEntry = {
  id: 'catalog-1',
  repoUrl: 'https://github.com/foo/bar',
  repoName: 'bar',
  projectSlug: 'foo/bar',
  lfCriticalityScore: null,
  source: 'manual',
}

function mockOptions() {
  return {
    log: { warn: vi.fn(), error: vi.fn() },
    database: { sequelize: {} },
  } as any
}

function lastSlackText(): string {
  const calls = vi.mocked(axios.post).mock.calls
  const message = calls[calls.length - 1][1] as { blocks: { text: { text: string } }[] }
  return message.blocks[0].text.text
}

describe('runOnboardProjectCommand', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(deriveProjectIdentityFromRepoUrl).mockReturnValue({
      projectSlug: 'foo/bar',
      repoName: 'bar',
    })
    vi.mocked(findProjectCatalogById).mockResolvedValue({ onboardedAt: null } as any)
  })

  it('evaluates and onboards a positively evaluated repo', async () => {
    vi.mocked(upsertProjectCatalogManualAction).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockResolvedValue({
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: null,
    })
    vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({
      ...catalogEntry,
      action: 'onboard',
    } as any)
    vi.mocked(onboardProject).mockResolvedValue({
      outcome: 'onboarded',
      segmentId: 'segment-1',
      error: null,
    })

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(onboardProject).toHaveBeenCalledTimes(1)
    expect(updateProjectCatalog).toHaveBeenCalledWith(
      expect.anything(),
      catalogEntry.id,
      expect.objectContaining({ action: 'onboarded', onboardingError: null }),
    )
    expect(lastSlackText()).toContain('onboarded successfully')
  })

  it('reports a negative evaluation and does not onboard', async () => {
    vi.mocked(upsertProjectCatalogManualAction).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockResolvedValue({
      outcome: 'skip',
      evaluationResult: 'false',
      evaluationReason: 'project is a documentation repo, an SDK, a website, a recipe, etc',
      metrics: null,
    })
    vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({
      ...catalogEntry,
      action: 'skip',
    } as any)

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(onboardProject).not.toHaveBeenCalled()
    expect(lastSlackText()).toContain('skip')
  })

  it('reports back without erroring when the repo is already onboarded or in flight', async () => {
    vi.mocked(upsertProjectCatalogManualAction).mockResolvedValue(null)

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(evaluateProject).not.toHaveBeenCalled()
    expect(lastSlackText()).toContain('already onboarded')
  })

  it('reports a clear message when the daily catalog cap is exhausted', async () => {
    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '1'
    try {
      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'daily-cap-actor',
      })
      expect(upsertProjectCatalogManualAction).toHaveBeenCalledTimes(1)

      vi.mocked(axios.post).mockClear()
      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'daily-cap-actor',
      })

      expect(upsertProjectCatalogManualAction).toHaveBeenCalledTimes(1)
      expect(lastSlackText()).toContain('no_entry')
    } finally {
      delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP
    }
  })
})
