import { describe, expect, it, vi } from 'vitest'

import {
  CatalogRowClassificationDeps,
  ICatalogRowToClassify,
  classifyCatalogRow,
  toCatalogClassification,
} from './classifyCatalogRow'
import {
  CdpSegmentLookupResult,
  ICdpSegmentMatch,
  IPccCandidate,
  OnboardingResolution,
} from './requestResolver'

const pccProject: IPccCandidate = {
  projectId: 'pcc-1',
  name: 'Acme',
  slug: 'acme',
  score: 1,
  isLeaf: true,
}

const segment: ICdpSegmentMatch = {
  segmentId: 'seg-1',
  name: 'Acme',
  slug: 'acme',
  integration: 'none',
}

interface IDepsState {
  candidates?: IPccCandidate[]
  cdpSegment?: CdpSegmentLookupResult
  trackedRepoUrls?: string[]
  lfOwners?: string[]
  llmAnswer?: Record<string, unknown> | string
  autoApply?: boolean
}

function deps({
  candidates = [],
  cdpSegment = null,
  trackedRepoUrls = [],
  lfOwners = [],
  llmAnswer = {},
  autoApply = false,
}: IDepsState = {}): CatalogRowClassificationDeps {
  const answer =
    typeof llmAnswer === 'string'
      ? llmAnswer
      : JSON.stringify({
          repoUrls: ['https://github.com/acme/one'],
          linkUrls: [],
          projectName: 'Acme',
          declaredLf: null,
          asksAboutHierarchy: false,
          ...llmAnswer,
        })

  return {
    queryLlm: vi.fn().mockResolvedValue(answer),
    autoApply,
    lookups: {
      findPccCandidates: vi.fn().mockResolvedValue(candidates),
      findCdpSegmentByPccProject: vi.fn().mockResolvedValue(cdpSegment),
      findTrackedRepoUrls: vi.fn().mockResolvedValue(new Set(trackedRepoUrls)),
      findExclusivelyLfOwners: vi.fn().mockResolvedValue(new Set(lfOwners)),
    },
  }
}

function row(overrides: Partial<ICatalogRowToClassify> = {}): ICatalogRowToClassify {
  return {
    repoUrl: 'https://github.com/acme/one',
    projectName: 'Acme',
    requestText: null,
    ...overrides,
  }
}

describe('classifyCatalogRow', () => {
  it('classifies a row from its repository URL without calling the LLM', async () => {
    const classifier = deps()

    const result = await classifyCatalogRow(row(), classifier)

    expect(classifier.queryLlm).not.toHaveBeenCalled()
    expect(result.trace.parsed).toEqual({
      githubRepoUrls: ['https://github.com/acme/one'],
      nonGithubRepoUrls: [],
      linksToFollow: [],
      projectName: 'Acme',
      declaredLf: null,
      asksAboutHierarchy: false,
    })
    expect(result.classification).toEqual({
      projectName: 'Acme',
      classificationKind: 'non_lf_new_project',
      proposedAction: 'create_segment',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'evaluate',
      requestQuestion: null,
      skipReason: null,
      reviewReason: null,
    })
  })

  it('canonicalizes the repository URL before looking it up', async () => {
    const classifier = deps({ trackedRepoUrls: ['https://github.com/acme/one'] })

    const result = await classifyCatalogRow(
      row({ repoUrl: 'https://GitHub.com/Acme/One.git' }),
      classifier,
    )

    expect(classifier.lookups.findTrackedRepoUrls).toHaveBeenCalledWith([
      'https://github.com/acme/one',
    ])
    expect(result.classification).toMatchObject({
      classificationKind: 'already_tracked',
      action: 'skip',
      requestQuestion: null,
    })
  })

  it('treats a non-GitHub repository URL as a non-GitHub source', async () => {
    const result = await classifyCatalogRow(row({ repoUrl: 'https://gitlab.com/acme/one' }), deps())

    expect(result.resolution).toEqual({
      kind: 'non_github_source',
      nonGithubRepoUrls: ['https://gitlab.com/acme/one'],
    })
    expect(result.classification.action).toBe('skip')
  })

  it('treats an unparseable repository URL as a non-GitHub source', async () => {
    const result = await classifyCatalogRow(row({ repoUrl: 'not a url' }), deps())

    expect(result.resolution).toEqual({
      kind: 'non_github_source',
      nonGithubRepoUrls: ['not a url'],
    })
  })

  it('parses the request text with the LLM when present', async () => {
    const classifier = deps({ llmAnswer: { declaredLf: true } })

    const result = await classifyCatalogRow(
      row({ requestText: 'Please onboard https://github.com/acme/one, it is an LF project' }),
      classifier,
    )

    expect(classifier.queryLlm).toHaveBeenCalled()
    expect(result.trace.parsed).toMatchObject({ declaredLf: true })
    expect(result.classification).toMatchObject({
      classificationKind: 'lf_not_in_pcc',
      proposedAction: 'human_review',
      action: 'review',
      reviewReason:
        "Acme is described as an LF project, but it isn't in the foundation project list yet",
      requestQuestion: null,
    })
  })

  it('asks a bot requester whether an unknown project is LF before evaluating it', async () => {
    const bot = { provenance: 'slack-bot' as const, requestedBy: 'U1' }

    const asked = await classifyCatalogRow(row(bot), deps())
    expect(asked.classification).toMatchObject({
      classificationKind: 'ambiguous',
      proposedAction: null,
      action: 'ask',
      requestQuestion: { kind: 'lf_confirm' },
    })

    const discovered = await classifyCatalogRow(row(), deps())
    expect(discovered.classification).toMatchObject({
      classificationKind: 'non_lf_new_project',
      proposedAction: 'create_segment',
      action: 'evaluate',
      requestQuestion: null,
    })

    const confirmedLf = await classifyCatalogRow(
      row({ ...bot, requestHints: { declaredLf: true } }),
      deps(),
    )
    expect(confirmedLf.classification).toMatchObject({
      classificationKind: 'lf_not_in_pcc',
      proposedAction: 'human_review',
      action: 'review',
    })

    const notLf = await classifyCatalogRow(
      row({ ...bot, requestHints: { treatAsNew: true } }),
      deps(),
    )
    expect(notLf.classification).toMatchObject({
      classificationKind: 'non_lf_new_project',
      action: 'evaluate',
    })
  })

  it('stores the project name the LLM extracted from the request text', async () => {
    const classifier = deps({ llmAnswer: { projectName: 'Acme Widgets' } })

    const result = await classifyCatalogRow(
      row({ projectName: null, requestText: 'Please onboard Acme Widgets' }),
      classifier,
    )

    expect(result.classification.projectName).toBe('Acme Widgets')
  })

  it('falls back to the row project name when the request text does not name the project', async () => {
    const classifier = deps({ llmAnswer: { projectName: null } })

    const result = await classifyCatalogRow(
      row({ requestText: 'Please onboard https://github.com/acme/one' }),
      classifier,
    )

    expect(result.trace.parsed).toMatchObject({ projectName: 'Acme' })
    expect(classifier.lookups.findPccCandidates).toHaveBeenCalledWith('Acme')
  })

  it('is ambiguous and traces the failure when the request text cannot be parsed', async () => {
    const result = await classifyCatalogRow(
      row({ requestText: 'Please onboard https://github.com/acme/one' }),
      deps({ llmAnswer: 'not json' }),
    )

    expect(result.trace.failure).toMatchObject({ stage: 'parse' })
    expect(result.classification).toMatchObject({
      classificationKind: 'ambiguous',
      proposedAction: null,
      action: 'skip',
      requestQuestion: null,
    })
  })

  it('rejects when a lookup throws so the caller can retry instead of skipping the row', async () => {
    const classifier = deps()
    classifier.lookups.findTrackedRepoUrls = vi.fn().mockRejectedValue(new Error('db down'))

    await expect(classifyCatalogRow(row(), classifier)).rejects.toThrow('db down')
  })

  it('rejects when the LLM throws while parsing the request text', async () => {
    const classifier = deps()
    classifier.queryLlm = vi.fn().mockRejectedValue(new Error('bedrock down'))

    await expect(
      classifyCatalogRow(
        row({ requestText: 'Please onboard https://github.com/acme/one' }),
        classifier,
      ),
    ).rejects.toThrow('bedrock down')
  })

  it('rejects when the LLM returns no response instead of skipping the row', async () => {
    const classifier = deps()
    classifier.queryLlm = vi.fn().mockResolvedValue(undefined)

    await expect(
      classifyCatalogRow(
        row({ requestText: 'Please onboard https://github.com/acme/one' }),
        classifier,
      ),
    ).rejects.toThrow('LLM query returned no response')
  })

  it('judges only the row repository when the request text lists several', async () => {
    const classifier = deps({
      llmAnswer: { repoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] },
      trackedRepoUrls: ['https://github.com/acme/one'],
    })

    const result = await classifyCatalogRow(
      row({
        requestText: 'Please onboard https://github.com/acme/one and https://github.com/acme/two',
      }),
      classifier,
    )

    expect(result.trace.parsed).toMatchObject({
      githubRepoUrls: ['https://github.com/acme/one'],
      nonGithubRepoUrls: [],
    })
    expect(classifier.lookups.findTrackedRepoUrls).toHaveBeenCalledWith([
      'https://github.com/acme/one',
    ])
    expect(result.classification).toMatchObject({
      classificationKind: 'already_tracked',
      action: 'skip',
      requestQuestion: null,
    })
  })

  it('records the PCC project and the integration action for an LF project in CDP', async () => {
    const result = await classifyCatalogRow(
      row(),
      deps({ candidates: [pccProject], cdpSegment: segment, autoApply: true }),
    )

    expect(result.classification).toEqual({
      projectName: 'Acme',
      classificationKind: 'lf_in_cdp',
      proposedAction: 'create_integration',
      pccProjectId: 'pcc-1',
      matchedProjectName: 'Acme',
      matchedSegmentId: 'seg-1',
      action: 'onboard',
      requestQuestion: null,
      skipReason: null,
      reviewReason: null,
    })
  })

  it('names the LFX Insights project when it differs from the PCC node that owns it', async () => {
    const parent = { ...pccProject, name: 'Software Supply Chain Security Project', isLeaf: false }
    const result = await classifyCatalogRow(
      row({ projectName: 'Uptane' }),
      deps({ candidates: [parent], cdpSegment: { ...segment, name: 'Uptane' } }),
    )

    expect(result.classification).toMatchObject({
      classificationKind: 'lf_in_cdp',
      pccProjectId: 'pcc-1',
      matchedProjectName: 'Uptane',
      matchedSegmentId: 'seg-1',
    })
  })

  it('routes an LF project in CDP to review when auto apply is off', async () => {
    const result = await classifyCatalogRow(
      row(),
      deps({ candidates: [pccProject], cdpSegment: segment }),
    )

    expect(result.classification).toMatchObject({
      action: 'review',
      requestQuestion: null,
      proposedAction: 'create_integration',
    })
  })

  it('maps an LF owner already in CDP to a human review of an LF project in CDP', async () => {
    const result = await classifyCatalogRow(row(), deps({ lfOwners: ['acme'] }))

    expect(result.resolution).toEqual({ kind: 'lf_owner_in_cdp', owners: ['acme'] })
    expect(result.classification).toEqual({
      projectName: 'Acme',
      classificationKind: 'lf_in_cdp',
      proposedAction: 'human_review',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      requestQuestion: null,
      skipReason: null,
      reviewReason:
        "the GitHub owner acme only has LF projects in LFX Insights, so I couldn't tell which one this repo belongs to",
    })
  })
})

describe('toCatalogClassification', () => {
  it('asks for human review of an LF project missing from CDP', () => {
    expect(
      toCatalogClassification({ kind: 'lf_not_in_cdp', pccProject }, { autoApply: true }),
    ).toEqual({
      classificationKind: 'lf_not_in_cdp',
      proposedAction: 'human_review',
      pccProjectId: 'pcc-1',
      matchedProjectName: 'Acme',
      matchedSegmentId: null,
      action: 'review',
      requestQuestion: null,
      skipReason: null,
      reviewReason: "Acme is an LF project, but it isn't set up in LFX Insights yet",
    })
  })

  it('asks for human review of an LF project missing from the foundation project list', () => {
    expect(
      toCatalogClassification({ kind: 'lf_not_in_pcc', projectName: 'Acme' }, { autoApply: true }),
    ).toEqual({
      classificationKind: 'lf_not_in_pcc',
      proposedAction: 'human_review',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      requestQuestion: null,
      skipReason: null,
      reviewReason:
        "Acme is described as an LF project, but it isn't in the foundation project list yet",
    })
  })

  it.each<OnboardingResolution>([
    { kind: 'already_tracked', repoUrls: ['https://github.com/acme/one'] },
    { kind: 'non_github_source', nonGithubRepoUrls: ['https://gitlab.com/acme/one'] },
    { kind: 'ambiguous', reason: 'x', candidates: [] },
  ])('proposes no action for $kind', (resolution) => {
    expect(toCatalogClassification(resolution, { autoApply: true })).toMatchObject({
      classificationKind: resolution.kind,
      proposedAction: null,
      pccProjectId: null,
      action: 'skip',
      requestQuestion: null,
    })
  })
})
