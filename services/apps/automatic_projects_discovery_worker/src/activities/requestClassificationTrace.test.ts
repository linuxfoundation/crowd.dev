import { describe, expect, it, vi } from 'vitest'

import {
  IOnboardingRequestLookups,
  IParsedOnboardingRequest,
  IPccCandidate,
  OnboardingResolution,
  PCC_MATCH_THRESHOLDS,
} from '@crowd/project-onboarding'

import {
  ClassificationNode,
  buildClassificationLogEntry,
  countNodes,
  createClassificationTrace,
  toClassificationNode,
  traceLookups,
} from './requestClassificationTrace'

const pccProject: IPccCandidate = {
  projectId: 'pcc-1',
  name: 'Acme',
  slug: 'acme',
  score: 0.9,
  isLeaf: true,
}

const parsed: IParsedOnboardingRequest = {
  githubRepoUrls: ['https://github.com/acme/one'],
  nonGithubRepoUrls: [],
  linksToFollow: [],
  projectName: 'Acme Labs',
  declaredLf: true,
  asksAboutHierarchy: false,
}

function lookups(overrides: Partial<IOnboardingRequestLookups> = {}): IOnboardingRequestLookups {
  return {
    findPccCandidates: vi.fn().mockResolvedValue([pccProject]),
    findCdpSegmentByPccProject: vi.fn().mockResolvedValue(null),
    ...overrides,
  }
}

describe('toClassificationNode', () => {
  it.each<[OnboardingResolution, ClassificationNode]>([
    [{ kind: 'non_github_source', nonGithubRepoUrls: [] }, 'not_github_source'],
    [{ kind: 'non_lf_new_project', projectName: 'Acme' }, 'non_lf_create_in_external_group'],
    [{ kind: 'lf_not_in_pcc', projectName: 'Acme' }, 'lf_not_in_pcc_flag_human'],
    [{ kind: 'lf_not_in_cdp', pccProject }, 'lf_in_pcc_not_in_cdp_human_review'],
    [{ kind: 'ambiguous', reason: 'x', candidates: [] }, 'ambiguous_human_review'],
  ])('maps %j to %s', (resolution, node) => {
    expect(toClassificationNode(resolution)).toBe(node)
  })

  it.each([
    ['create_integration', 'lf_in_cdp_integration_none_create'],
    ['update_integration', 'lf_in_cdp_github_nango_update'],
    ['human_review', 'lf_in_cdp_github_v1_human_review'],
  ] as const)('maps the %s action to %s', (action, node) => {
    const segment = { segmentId: 'seg-1', name: 'Acme', integration: 'none' as const }

    expect(toClassificationNode({ kind: 'lf_in_cdp', pccProject, segment, action })).toBe(node)
  })
})

describe('traceLookups', () => {
  it('records the PCC candidates and the CDP result while passing them through', async () => {
    const trace = createClassificationTrace()
    const traced = traceLookups(lookups(), trace)

    expect(await traced.findPccCandidates?.('Acme')).toEqual([pccProject])
    expect(await traced.findCdpSegmentByPccProject('pcc-1')).toBeNull()

    expect(trace.pccLookup).toEqual({ projectName: 'Acme', candidates: [pccProject] })
    expect(trace.cdpLookup).toEqual({ result: null })
  })

  it('keeps the PCC lookup missing when it is not configured', () => {
    const traced = traceLookups(
      lookups({ findPccCandidates: undefined }),
      createClassificationTrace(),
    )

    expect(traced.findPccCandidates).toBeUndefined()
  })
})

describe('buildClassificationLogEntry', () => {
  it('reports the request, the threshold assessment and the node reached', () => {
    const trace = createClassificationTrace()
    trace.parsed = parsed
    trace.pccLookup = { projectName: 'Acme Labs', candidates: [pccProject] }

    const entry = buildClassificationLogEntry(
      'https://github.com/linuxfoundation/insights/discussions/1',
      {
        kind: 'ambiguous',
        reason: 'Project name only loosely matches PCC projects',
        candidates: [pccProject],
      },
      trace,
    )

    expect(entry).toMatchObject({
      node: 'ambiguous_human_review',
      kind: 'ambiguous',
      failure: null,
      request: { githubRepos: 1, projectName: 'Acme Labs', declaredLf: true },
      pcc: {
        level: 'weak',
        thresholds: PCC_MATCH_THRESHOLDS,
        best: pccProject,
        candidates: [pccProject],
      },
      cdp: null,
    })
  })

  it('leaves the PCC and CDP sections empty when nothing was looked up', () => {
    const trace = createClassificationTrace()
    trace.failure = { stage: 'parse', reason: 'LLM did not answer' }

    const entry = buildClassificationLogEntry(
      'url',
      { kind: 'ambiguous', reason: 'x', candidates: [] },
      trace,
    )

    expect(entry).toMatchObject({
      request: null,
      pcc: null,
      cdp: null,
      failure: { stage: 'parse', reason: 'LLM did not answer' },
    })
  })

  it('never includes the request text', () => {
    const trace = createClassificationTrace()
    trace.parsed = parsed

    const entry = buildClassificationLogEntry(
      'url',
      { kind: 'non_lf_new_project', projectName: 'Acme' },
      trace,
    )

    expect(JSON.stringify(entry)).not.toContain('github.com/acme/one')
  })
})

describe('countNodes', () => {
  it('counts the discussions per node', () => {
    expect(
      countNodes(['not_github_source', 'ambiguous_human_review', 'ambiguous_human_review']),
    ).toEqual({ not_github_source: 1, ambiguous_human_review: 2 })
  })
})
