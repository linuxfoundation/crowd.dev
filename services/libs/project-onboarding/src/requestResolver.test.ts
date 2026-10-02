import { describe, expect, it, vi } from 'vitest'

import { IParsedOnboardingRequest } from './requestParser'
import {
  ICdpSegmentMatch,
  IOnboardingRequestLookups,
  IPccCandidate,
  resolveOnboardingRequest,
  toCdpIntegrationState,
} from './requestResolver'

function request(overrides: Partial<IParsedOnboardingRequest> = {}): IParsedOnboardingRequest {
  return {
    githubRepoUrls: ['https://github.com/acme/one'],
    nonGithubRepoUrls: [],
    linksToFollow: [],
    projectName: 'Acme',
    declaredLf: null,
    asksAboutHierarchy: false,
    ...overrides,
  }
}

function candidate(overrides: Partial<IPccCandidate> = {}): IPccCandidate {
  return { projectId: 'pcc-1', name: 'Acme', slug: 'acme', score: 1, ...overrides }
}

function segment(overrides: Partial<ICdpSegmentMatch> = {}): ICdpSegmentMatch {
  return { segmentId: 'seg-1', name: 'Acme', integration: 'none', ...overrides }
}

function lookups(
  candidates: IPccCandidate[],
  cdpSegment: ICdpSegmentMatch | null = null,
): IOnboardingRequestLookups {
  return {
    findPccCandidates: vi.fn().mockResolvedValue(candidates),
    findCdpSegmentByPccProject: vi.fn().mockResolvedValue(cdpSegment),
  }
}

describe('resolveOnboardingRequest', () => {
  it('proposes a new project when nothing matches PCC and the request is not declared LF', async () => {
    const result = await resolveOnboardingRequest(request(), lookups([]))

    expect(result).toEqual({ kind: 'non_lf_new_project', projectName: 'Acme' })
  })

  it('alerts when the request is declared LF but PCC has no match', async () => {
    const result = await resolveOnboardingRequest(request({ declaredLf: true }), lookups([]))

    expect(result).toEqual({ kind: 'lf_not_in_pcc', projectName: 'Acme' })
  })

  it('reports an LF project that exists in PCC but not in CDP', async () => {
    const pccProject = candidate()

    const result = await resolveOnboardingRequest(request(), lookups([pccProject]))

    expect(result).toEqual({ kind: 'lf_not_in_cdp', pccProject })
  })

  it.each([
    ['none', 'create_integration'],
    ['github-nango', 'update_integration'],
    ['github-v1', 'human_review'],
  ] as const)('maps the %s integration to %s', async (integration, action) => {
    const pccProject = candidate()
    const cdpSegment = segment({ integration })

    const result = await resolveOnboardingRequest(request(), lookups([pccProject], cdpSegment))

    expect(result).toEqual({ kind: 'lf_in_cdp', pccProject, segment: cdpSegment, action })
  })

  it('treats an exact name match as strong even with a low score', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'ACME' }),
      lookups([candidate({ score: 0.5 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('treats a high similarity score as strong', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'Acme Project' }),
      lookups([candidate({ name: 'Acme Projects', slug: 'acme-projects', score: 0.98 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('returns candidates for a human when the match is weak', async () => {
    const weak = candidate({ name: 'Acme Labs', slug: 'acme-labs', score: 0.9 })

    const result = await resolveOnboardingRequest(request(), lookups([weak]))

    expect(result).toMatchObject({ kind: 'ambiguous', candidates: [weak] })
  })

  it('ignores candidates below the weak threshold', async () => {
    const result = await resolveOnboardingRequest(
      request(),
      lookups([candidate({ name: 'Other', slug: 'other', score: 0.4 })]),
    )

    expect(result.kind).toBe('non_lf_new_project')
  })

  it('is ambiguous when the request contradicts a strong PCC match', async () => {
    const result = await resolveOnboardingRequest(
      request({ declaredLf: false }),
      lookups([candidate()]),
    )

    expect(result.kind).toBe('ambiguous')
  })

  it('does not look up CDP when the PCC match is not strong', async () => {
    const deps = lookups([candidate({ name: 'Acme Labs', slug: 'acme-labs', score: 0.9 })])

    await resolveOnboardingRequest(request(), deps)

    expect(deps.findCdpSegmentByPccProject).not.toHaveBeenCalled()
  })

  it('is ambiguous when the requester asks about the hierarchy', async () => {
    const deps = lookups([candidate()])

    const result = await resolveOnboardingRequest(request({ asksAboutHierarchy: true }), deps)

    expect(result.kind).toBe('ambiguous')
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('is ambiguous when there are no repositories and no links', async () => {
    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: [] }),
      lookups([candidate()]),
    )

    expect(result.kind).toBe('ambiguous')
  })

  it('is ambiguous when the PCC lookup is not configured', async () => {
    const result = await resolveOnboardingRequest(request(), {
      findCdpSegmentByPccProject: vi.fn(),
    })

    expect(result).toEqual({
      kind: 'ambiguous',
      reason: 'PCC lookup is not configured',
      candidates: [],
    })
  })

  it('is ambiguous when the project name is unknown', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: null }),
      lookups([candidate()]),
    )

    expect(result.kind).toBe('ambiguous')
  })
})

describe('toCdpIntegrationState', () => {
  it.each([
    [[], 'none'],
    [['slack', 'discord'], 'none'],
    [['github-nango'], 'github-nango'],
    [['github'], 'github-v1'],
    [['github-nango', 'github'], 'github-v1'],
  ] as const)('maps platforms %j to %s', (platforms, expected) => {
    expect(toCdpIntegrationState([...platforms])).toBe(expected)
  })
})
