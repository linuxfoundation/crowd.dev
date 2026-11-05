import { describe, expect, it, vi } from 'vitest'

import {
  IDbProjectCatalog,
  IDbProjectCatalogClassification,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { OnboardingConfigurationError } from '@crowd/project-onboarding'

import { IClassifyAndRouteIo, classifyAndRouteProjects } from './classifyAndRoute'

function project(overrides: Partial<IDbProjectCatalog> = {}): IDbProjectCatalog {
  return {
    id: 'id-1',
    projectSlug: 'acme-widget',
    repoName: 'acme/widget',
    repoUrl: 'https://github.com/acme/widget',
    source: 'manual',
    sourceUrl: null,
    provenance: null,
    action: 'evaluate',
    lfCriticalityScore: null,
    evaluationResult: null,
    evaluationReason: null,
    evaluationAttempts: 0,
    evaluatedAt: null,
    onboardedAt: null,
    onboardingError: null,
    skipReason: null,
    requestText: null,
    projectName: null,
    classificationKind: null,
    proposedAction: null,
    pccProjectId: null,
    matchedProjectName: null,
    matchedSegmentId: null,
    decisionReason: null,
    decisionNote: null,
    requestedBy: null,
    requestQuestion: null,
    requestHints: null,
    classifiedAt: null,
    reviewedAt: null,
    reviewedBy: null,
    reviewMessageTs: null,
    reviewReason: null,
    syncedAt: null,
    createdAt: null,
    updatedAt: null,
    ...overrides,
  }
}

function classification(
  overrides: Partial<IDbProjectCatalogClassification> = {},
): IDbProjectCatalogClassification {
  return {
    projectName: null,
    classificationKind: 'non_lf_new_project',
    proposedAction: 'create_segment',
    pccProjectId: null,
    matchedProjectName: null,
    matchedSegmentId: null,
    action: 'evaluate',
    skipReason: null,
    reviewReason: null,
    ...overrides,
  }
}

function createIo(overrides: Partial<IClassifyAndRouteIo> = {}) {
  const io = {
    findFresh: vi.fn(async (id: string) => project({ id })),
    classify: vi.fn(async () => classification()),
    markClassified: vi.fn(async (id: string, data: IDbProjectCatalogClassification) =>
      project({ ...data, id, classifiedAt: '2026-10-12T04:00:00Z' }),
    ),
    markFailed: vi.fn(async () => 1),
    findTrackedRepoUrls: vi.fn(async () => new Set<string>()),
    onRowStart: vi.fn(),
    ...overrides,
  }

  return io
}

describe('classifyAndRouteProjects', () => {
  it('keeps rows routed to evaluate and persists their classification', async () => {
    const io = createIo()

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).toHaveBeenCalledWith({
      repoUrl: 'https://github.com/acme/widget',
      projectName: null,
      requestText: null,
      provenance: null,
      requestedBy: null,
      requestHints: null,
    })
    expect(io.markClassified).toHaveBeenCalledWith('id-1', classification())
    expect(result.remaining.map((row) => row.id)).toEqual(['id-1'])
    expect(result.routed).toEqual({ evaluate: 1, ask: 0, review: 0, onboard: 0, skip: 0 })
    expect(result.breakdown).toEqual({ non_lf_new_project: 1 })
    expect(result.discarded).toBe(0)
  })

  it('classifies the repositories of one organization request once', async () => {
    const source = 'https://lfx.slack.com/archives/C1/p1'
    const rows = ['id-1', 'id-2', 'id-3'].map((id) =>
      project({ id, sourceUrl: source, requestHints: { organization: 'acme' } }),
    )
    const io = createIo({
      findFresh: vi.fn(async (id: string) => rows.find((row) => row.id === id)!),
      markClassified: vi.fn(async (id: string, data: IDbProjectCatalogClassification) =>
        project({
          ...rows.find((row) => row.id === id)!,
          ...data,
          classifiedAt: '2026-10-12T04:00:00Z',
        }),
      ),
    })

    const result = await classifyAndRouteProjects(rows, io)

    expect(io.classify).toHaveBeenCalledTimes(1)
    expect(io.markClassified).toHaveBeenCalledTimes(3)
    expect(result.routed).toEqual({ evaluate: 3, ask: 0, review: 0, onboard: 0, skip: 0 })
  })

  it('classifies a tracked repository of an organization on its own and never shares a tracked lead', async () => {
    const source = 'https://lfx.slack.com/archives/C1/p1'
    const rows = ['id-1', 'id-2', 'id-3'].map((id) =>
      project({
        id,
        repoUrl: `https://github.com/acme/${id}`,
        sourceUrl: source,
        requestHints: { organization: 'acme' },
      }),
    )
    const tracked = new Set(['https://github.com/acme/id-1', 'https://github.com/acme/id-3'])
    const io = createIo({
      findFresh: vi.fn(async (id: string) => rows.find((row) => row.id === id)!),
      markClassified: vi.fn(async (id: string, data: IDbProjectCatalogClassification) =>
        project({
          ...rows.find((row) => row.id === id)!,
          ...data,
          classifiedAt: '2026-10-12T04:00:00Z',
        }),
      ),
      findTrackedRepoUrls: vi.fn(
        async (urls: string[]) => new Set(urls.filter((url) => tracked.has(url))),
      ),
      classify: vi.fn(async (row) =>
        tracked.has(row.repoUrl)
          ? classification({
              classificationKind: 'already_tracked',
              proposedAction: null,
              action: 'skip',
              skipReason: 'it is already in LFX Insights',
            })
          : classification({
              classificationKind: 'lf_in_cdp',
              proposedAction: 'create_integration',
              action: 'review',
              reviewReason: 'needs a check',
            }),
      ),
    })

    const result = await classifyAndRouteProjects(rows, io)

    expect(io.classify).toHaveBeenCalledTimes(3)
    expect(result.routed).toEqual({ evaluate: 0, ask: 0, review: 1, onboard: 0, skip: 2 })
  })

  it('reuses the classification another run already persisted for the organization', async () => {
    const source = 'https://lfx.slack.com/archives/C1/p1'
    const question = { kind: 'did_you_mean' as const, text: 'Did you mean Acme?', options: [] }
    const lead = project({
      id: 'id-1',
      sourceUrl: source,
      requestHints: { organization: 'acme' },
      action: 'ask',
      classifiedAt: '2026-10-19T04:00:00Z',
      classificationKind: 'ambiguous',
      requestQuestion: question,
    })
    const sibling = project({
      id: 'id-2',
      sourceUrl: source,
      requestHints: { organization: 'acme' },
    })
    const io = createIo({
      findFresh: vi.fn(async (id: string) => (id === 'id-1' ? lead : sibling)),
    })

    const result = await classifyAndRouteProjects([lead, sibling], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(io.markClassified).toHaveBeenCalledWith(
      'id-2',
      expect.objectContaining({
        action: 'ask',
        classificationKind: 'ambiguous',
        requestQuestion: question,
      }),
    )
    expect(result.routed.ask).toBe(2)
  })

  it('shares the classification of an answered organization but not a question still waiting', async () => {
    const source = 'https://lfx.slack.com/archives/C1/p1'
    const question = { kind: 'did_you_mean' as const, text: 'Did you mean Acme?', options: [] }
    const base = { sourceUrl: source, requestHints: { organization: 'acme' } }
    const waiting = project({
      ...base,
      id: 'id-1',
      action: 'ask',
      classifiedAt: '2026-10-19T04:00:00Z',
      requestQuestion: { ...question, messageTs: '1760000000.300000' },
    })
    const answered = project({
      ...base,
      id: 'id-2',
      action: 'review',
      proposedAction: 'create_integration',
      classifiedAt: '2026-10-19T05:00:00Z',
      requestQuestion: { ...question, messageTs: '1760000000.300000', answer: 'yes' },
    })
    const sibling = project({ ...base, id: 'id-3' })
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        [waiting, answered, sibling].find((row) => row.id === id)!,
      ),
    })

    await classifyAndRouteProjects([waiting, answered, sibling], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(io.markClassified).toHaveBeenCalledWith(
      'id-3',
      expect.objectContaining({ action: 'review', proposedAction: 'create_integration' }),
    )
  })

  it('treats an organization with a single repository like a plain repository', async () => {
    const row = project({
      id: 'id-1',
      sourceUrl: 'https://lfx.slack.com/archives/C1/p1',
      requestHints: { organization: 'acme' },
    })
    const io = createIo({ findFresh: vi.fn(async () => row) })

    const result = await classifyAndRouteProjects([row], io)

    expect(io.markClassified).toHaveBeenCalledWith('id-1', classification())
    expect(result.routed.evaluate).toBe(1)
  })

  it('sends a new non-LF organization to evaluation with its main repository first', async () => {
    const source = 'https://lfx.slack.com/archives/C1/p1'
    const rows = ['tools', 'acme', 'cli'].map((name) =>
      project({
        id: `id-${name}`,
        repoUrl: `https://github.com/acme/${name}`,
        sourceUrl: source,
        requestHints: { organization: 'acme' },
      }),
    )
    const io = createIo({
      findFresh: vi.fn(async (id: string) => rows.find((row) => row.id === id)!),
      markClassified: vi.fn(async (id: string, data: IDbProjectCatalogClassification) =>
        project({
          ...rows.find((row) => row.id === id)!,
          ...data,
          classifiedAt: '2026-10-12T04:00:00Z',
        }),
      ),
      classify: vi.fn(async () => classification({ projectName: 'Acme' })),
    })

    const result = await classifyAndRouteProjects(rows, io)

    expect(io.classify).toHaveBeenCalledTimes(1)
    expect(io.markClassified).toHaveBeenCalledWith(
      'id-cli',
      classification({ projectName: 'Acme', requestQuestion: null }),
    )
    expect(result.remaining.map((row) => row.id)).toEqual(['id-acme', 'id-tools', 'id-cli'])
    expect(result.routed.evaluate).toBe(3)
  })

  it('classifies from the fresh row rather than the workflow snapshot', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({ id, projectName: 'Widget', requestText: 'please onboard Widget' }),
      ),
    })

    await classifyAndRouteProjects([project()], io)

    expect(io.classify).toHaveBeenCalledWith({
      repoUrl: 'https://github.com/acme/widget',
      projectName: 'Widget',
      requestText: 'please onboard Widget',
      provenance: null,
      requestedBy: null,
      requestHints: null,
    })
  })

  it('routes review, onboard and skip rows out of the evaluation queue', async () => {
    const outcomes: Record<string, IDbProjectCatalogClassification> = {
      'id-review': classification({
        classificationKind: 'lf_not_in_cdp',
        proposedAction: 'human_review',
        action: 'review',
      }),
      'id-onboard': classification({
        classificationKind: 'lf_in_cdp',
        proposedAction: 'create_integration',
        action: 'onboard',
      }),
      'id-skip': classification({
        classificationKind: 'already_tracked',
        proposedAction: null,
        action: 'skip',
        skipReason: 'repository already tracked in CDP',
        reviewReason: null,
      }),
    }
    let current = ''
    const io = createIo({
      findFresh: vi.fn(async (id: string) => {
        current = id
        return project({ id })
      }),
      classify: vi.fn(async () => outcomes[current]),
    })

    const result = await classifyAndRouteProjects(
      [project({ id: 'id-review' }), project({ id: 'id-onboard' }), project({ id: 'id-skip' })],
      io,
    )

    expect(result.remaining).toEqual([])
    expect(result.routed).toEqual({ evaluate: 0, ask: 0, review: 1, onboard: 1, skip: 1 })
    expect(result.breakdown).toEqual({ lf_not_in_cdp: 1, lf_in_cdp: 1, already_tracked: 1 })
    expect(result.skippedHumanRequests).toEqual([])
  })

  it('collects skipped rows with a review-alert provenance for the Slack alert', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({ id, provenance: 'github-discussion', sourceUrl: 'https://example/discussion' }),
      ),
      classify: vi.fn(async () =>
        classification({
          classificationKind: 'non_github_source',
          proposedAction: null,
          action: 'skip',
          skipReason: 'no GitHub repository',
          reviewReason: null,
        }),
      ),
      markClassified: vi.fn(async (id: string, data: IDbProjectCatalogClassification) =>
        project({ ...data, id, provenance: 'github-discussion' }),
      ),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(result.skippedHumanRequests).toEqual([
      { project: expect.objectContaining({ id: 'id-1' }), reason: 'no GitHub repository' },
    ])
  })

  it('does not alert for skipped bulk rows', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) => project({ id, provenance: 'lf-criticality-score' })),
      classify: vi.fn(async () =>
        classification({ classificationKind: 'already_tracked', action: 'skip', skipReason: 'x' }),
      ),
      markClassified: vi.fn(async (id: string, data: IDbProjectCatalogClassification) =>
        project({ ...data, id, provenance: 'lf-criticality-score' }),
      ),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(result.routed.skip).toBe(1)
    expect(result.skippedHumanRequests).toEqual([])
  })

  it('passes already-classified evaluate rows through without a new classification', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({
          id,
          classifiedAt: '2026-10-11T04:00:00Z',
          classificationKind: 'non_lf_new_project',
        }),
      ),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(io.markClassified).not.toHaveBeenCalled()
    expect(result.remaining.map((row) => row.id)).toEqual(['id-1'])
    expect(result.routed.evaluate).toBe(1)
    expect(result.breakdown).toEqual({ non_lf_new_project: 1 })
  })

  it('restores rows routed by a previous attempt instead of discarding them', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({
          id,
          action: 'skip',
          classifiedAt: '2026-10-12T04:00:00Z',
          classificationKind: 'already_tracked',
          skipReason: 'repository already tracked in CDP',
          provenance: 'github-discussion',
        }),
      ),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(io.markClassified).not.toHaveBeenCalled()
    expect(result.discarded).toBe(0)
    expect(result.routed).toEqual({ evaluate: 0, ask: 0, review: 0, onboard: 0, skip: 1 })
    expect(result.breakdown).toEqual({ already_tracked: 1 })
    expect(result.skippedHumanRequests).toEqual([
      {
        project: expect.objectContaining({ id: 'id-1' }),
        reason: 'repository already tracked in CDP',
      },
    ])
  })

  it('still owes the notice for a failure recorded by an earlier attempt', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({
          id,
          provenance: 'slack-bot',
          action: 'error',
          evaluationResult: 'error',
          evaluationReason: 'the lookup is not set up',
        }),
      ),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(io.markFailed).not.toHaveBeenCalled()
    expect(result.failed).toBe(1)
    expect(result.discarded).toBe(0)
    expect(result.erroredHumanRequests).toEqual([
      { project: expect.objectContaining({ id: 'id-1' }), reason: 'the lookup is not set up' },
    ])
  })

  it('discards rows moved out of evaluate before classification', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) => project({ id, action: 'onboard' })),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(result.remaining).toEqual([])
    expect(result.discarded).toBe(1)
  })

  it('discards rows whose guarded write did not apply', async () => {
    const io = createIo({ markClassified: vi.fn(async () => null) })

    const result = await classifyAndRouteProjects([project()], io)

    expect(result.remaining).toEqual([])
    expect(result.routed).toEqual({ evaluate: 0, ask: 0, review: 0, onboard: 0, skip: 0 })
    expect(result.discarded).toBe(1)
  })

  it('routes rows that wait for the requester out of the evaluation queue', async () => {
    const question = { kind: 'did_you_mean' as const, text: 'Did you mean Acme?', options: [] }
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({ id, provenance: 'slack-bot', requestedBy: 'U1' }),
      ),
      classify: vi.fn(async () => classification({ action: 'ask', requestQuestion: question })),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).toHaveBeenCalledWith(
      expect.objectContaining({ provenance: 'slack-bot', requestedBy: 'U1' }),
    )
    expect(result.routed.ask).toBe(1)
    expect(result.remaining).toEqual([])
  })

  it('restores an asked row routed by a previous attempt', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) =>
        project({ id, action: 'ask', classifiedAt: '2026-10-12T04:00:00Z' }),
      ),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(io.classify).not.toHaveBeenCalled()
    expect(result.routed.ask).toBe(1)
  })

  it('marks a configuration error on the row and keeps the batch going', async () => {
    const io = createIo({
      findFresh: vi.fn(async (id: string) => project({ id, provenance: 'slack-bot' })),
      classify: vi
        .fn()
        .mockRejectedValueOnce(new OnboardingConfigurationError('the lookup is not set up'))
        .mockResolvedValueOnce(classification()),
    })

    const result = await classifyAndRouteProjects(
      [project({ id: 'id-1' }), project({ id: 'id-2' })],
      io,
    )

    expect(io.markFailed).toHaveBeenCalledWith('id-1', 'the lookup is not set up')
    expect(io.markClassified).toHaveBeenCalledTimes(1)
    expect(result.failed).toBe(1)
    expect(result.erroredHumanRequests).toEqual([
      { project: expect.objectContaining({ id: 'id-1' }), reason: 'the lookup is not set up' },
    ])
    expect(result.remaining.map((row) => row.id)).toEqual(['id-2'])
  })

  it('does not collect configuration errors of bulk rows for the thread', async () => {
    const io = createIo({
      classify: vi.fn().mockRejectedValueOnce(new OnboardingConfigurationError('not set up')),
    })

    const result = await classifyAndRouteProjects([project()], io)

    expect(result.failed).toBe(1)
    expect(result.erroredHumanRequests).toEqual([])
  })

  it('heartbeats before every row and propagates classification failures', async () => {
    const io = createIo({
      classify: vi.fn(async () => {
        throw new Error('LLM unavailable')
      }),
    })

    await expect(classifyAndRouteProjects([project()], io)).rejects.toThrow('LLM unavailable')
    expect(io.onRowStart).toHaveBeenCalledTimes(1)
    expect(io.markClassified).not.toHaveBeenCalled()
  })
})
