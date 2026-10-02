import { describe, expect, it, vi } from 'vitest'

import { IParsedOnboardingRequest } from './requestParser'
import {
  ICdpSegmentMatch,
  IOnboardingRequestLookups,
  IPccCandidate,
  PCC_MATCH_THRESHOLDS,
  assessPccCandidates,
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
  return { projectId: 'pcc-1', name: 'Acme', slug: 'acme', score: 1, isLeaf: true, ...overrides }
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

  it('is ambiguous when the repositories can only be found in linked pages', async () => {
    const deps = lookups([candidate()])

    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: [], linksToFollow: ['https://acme.org/projects'] }),
      deps,
    )

    expect(result).toEqual({
      kind: 'ambiguous',
      reason: 'Repositories must be read from linked pages, which are not followed yet',
      candidates: [],
    })
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('does not treat names that differ by meaningful symbols as an exact match', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'C++' }),
      lookups([candidate({ name: 'C', slug: 'c', score: 0.5 })]),
    )

    expect(result.kind).toBe('non_lf_new_project')
  })

  it('treats names that differ only by case and separators as an exact match', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'C++ Tools' }),
      lookups([candidate({ name: 'c++-tools', slug: 'cpp-tools', score: 0.5 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('does not look up CDP when the PCC match is not strong', async () => {
    const deps = lookups([candidate({ name: 'Acme Labs', slug: 'acme-labs', score: 0.9 })])

    await resolveOnboardingRequest(request(), deps)

    expect(deps.findCdpSegmentByPccProject).not.toHaveBeenCalled()
  })

  it('cannot onboard a request that only lists non-GitHub repositories', async () => {
    const deps = lookups([candidate()])

    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: [], nonGithubRepoUrls: ['https://gitlab.com/acme/tool'] }),
      deps,
    )

    expect(result).toEqual({
      kind: 'non_github_source',
      nonGithubRepoUrls: ['https://gitlab.com/acme/tool'],
    })
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('keeps going when GitHub repositories come with non-GitHub ones', async () => {
    const result = await resolveOnboardingRequest(
      request({ nonGithubRepoUrls: ['https://gitlab.com/acme/tool'] }),
      lookups([]),
    )

    expect(result.kind).toBe('non_lf_new_project')
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

  it('does not treat unrelated non-Latin names as an exact match', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: '北京' }),
      lookups([candidate({ name: '東京', slug: '東京', score: 0.3 })]),
    )

    expect(result.kind).toBe('non_lf_new_project')
  })

  it('treats the same non-Latin name as an exact match', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: '東京' }),
      lookups([candidate({ name: '東京', slug: '東京', score: 0.3 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('is ambiguous when several leaf projects tie for the best score', async () => {
    const first = candidate({ projectId: 'pcc-1' })
    const second = candidate({ projectId: 'pcc-2' })
    const deps = lookups([first, second])

    const result = await resolveOnboardingRequest(request(), deps)

    expect(result).toMatchObject({ kind: 'ambiguous', candidates: [first, second] })
    expect(deps.findCdpSegmentByPccProject).not.toHaveBeenCalled()
  })

  it('prefers the leaf project over a parent with the same name', async () => {
    const parent = candidate({ projectId: 'pcc-parent', isLeaf: false })
    const leaf = candidate({ projectId: 'pcc-leaf' })

    const result = await resolveOnboardingRequest(request(), lookups([parent, leaf]))

    expect(result).toEqual({ kind: 'lf_not_in_cdp', pccProject: leaf })
  })

  it('is ambiguous when the best match is a PCC parent project', async () => {
    const parent = candidate({ isLeaf: false })
    const deps = lookups([parent])

    const result = await resolveOnboardingRequest(request(), deps)

    expect(result).toMatchObject({ kind: 'ambiguous', candidates: [parent] })
    expect(deps.findCdpSegmentByPccProject).not.toHaveBeenCalled()
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

describe('assessPccCandidates', () => {
  it('reports an exact name match regardless of the score', () => {
    const best = candidate({ score: 0.5 })

    expect(assessPccCandidates('ACME', [best])).toMatchObject({ level: 'exact', best })
  })

  it('reports a strong match at the strong threshold', () => {
    const best = candidate({ name: 'Acme Projects', slug: 'acme-projects', score: 0.97 })

    expect(assessPccCandidates('Acme Project', [best]).level).toBe('strong')
  })

  it('reports a weak match between the weak and strong thresholds', () => {
    const best = candidate({ name: 'Acme Labs', slug: 'acme-labs', score: 0.9 })

    expect(assessPccCandidates('Acme', [best])).toMatchObject({
      level: 'weak',
      weakCandidates: [best],
    })
  })

  it('reports no match below the weak threshold but keeps the best candidate', () => {
    const best = candidate({ name: 'Other', slug: 'other', score: 0.4 })

    expect(assessPccCandidates('Acme', [best])).toMatchObject({
      level: 'none',
      best,
      weakCandidates: [],
    })
  })

  it('reports the margin between the best and the runner-up', () => {
    const best = candidate({ name: 'Acme Labs', slug: 'acme-labs', score: 0.92 })
    const runnerUp = candidate({ projectId: 'pcc-2', name: 'Acme X', slug: 'acme-x', score: 0.88 })

    const assessment = assessPccCandidates('Acme', [runnerUp, best])

    expect(assessment.best).toBe(best)
    expect(assessment.margin).toBeCloseTo(0.04)
  })

  it('has no best candidate and no margin without candidates', () => {
    expect(assessPccCandidates('Acme', [])).toEqual({
      level: 'none',
      best: null,
      weakCandidates: [],
      tiedCandidates: [],
      margin: null,
      thresholds: PCC_MATCH_THRESHOLDS,
    })
  })
})
