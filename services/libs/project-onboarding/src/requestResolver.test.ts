import { describe, expect, it, vi } from 'vitest'

import { OnboardingConfigurationError } from './errors'
import { IParsedOnboardingRequest } from './requestParser'
import {
  CdpSegmentLookupResult,
  ICdpSegmentMatch,
  IOnboardingRequestLookups,
  IPccCandidate,
  PCC_MATCH_THRESHOLDS,
  assessPccCandidates,
  coreProjectName,
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
  return { segmentId: 'seg-1', name: 'Acme', slug: 'acme', integration: 'none', ...overrides }
}

interface ICdpState {
  trackedRepoUrls?: string[]
  lfOwners?: string[]
}

function lookups(
  candidates: IPccCandidate[],
  cdpSegment: CdpSegmentLookupResult = null,
  { trackedRepoUrls = [], lfOwners = [] }: ICdpState = {},
): IOnboardingRequestLookups {
  return {
    findPccCandidates: vi.fn().mockResolvedValue(candidates),
    findCdpSegmentByPccProject: vi.fn().mockResolvedValue(cdpSegment),
    findTrackedRepoUrls: vi.fn().mockResolvedValue(new Set(trackedRepoUrls)),
    findExclusivelyLfOwners: vi.fn().mockResolvedValue(new Set(lfOwners)),
  }
}

function withoutPcc(deps: IOnboardingRequestLookups): IOnboardingRequestLookups {
  return { ...deps, findPccCandidates: undefined }
}

describe('resolveOnboardingRequest', () => {
  it('proposes a new project when nothing matches PCC and the request is not declared LF', async () => {
    const result = await resolveOnboardingRequest(request(), lookups([]))

    expect(result).toEqual({ kind: 'non_lf_new_project', projectName: 'Acme' })
  })

  it('asks the requester whether an unknown project is LF when it can', async () => {
    const result = await resolveOnboardingRequest(request(), lookups([]), {}, { canAsk: true })

    expect(result).toEqual({
      kind: 'needs_input',
      candidates: [],
      fallbackReason:
        "Acme isn't in the foundation project list and the request doesn't say whether it is an LF project",
      question: {
        kind: 'lf_confirm',
        text: "I couldn't find Acme in the foundation project list. Is it a Linux Foundation project?",
        options: [
          { id: 'lf', label: "Yes, it's an LF project", hints: { declaredLf: true } },
          { id: 'not_lf', label: "No, it's not LF", hints: { treatAsNew: true } },
          { id: 'not_sure', label: 'Not sure, ask the CDP team', handOff: true },
        ],
      },
    })
  })

  it('does not ask about the LF status when the request already says it or has no name', async () => {
    const ask = { canAsk: true }

    expect(
      await resolveOnboardingRequest(request({ declaredLf: false }), lookups([]), {}, ask),
    ).toEqual({ kind: 'non_lf_new_project', projectName: 'Acme' })
    expect(
      await resolveOnboardingRequest(request({ declaredLf: true }), lookups([]), {}, ask),
    ).toEqual({ kind: 'lf_not_in_pcc', projectName: 'Acme' })
    expect(
      await resolveOnboardingRequest(request({ projectName: null }), lookups([]), {}, ask),
    ).toEqual({ kind: 'non_lf_new_project', projectName: null })
  })

  it('follows the LF answer on the next run', async () => {
    const ask = { canAsk: true }

    expect(
      await resolveOnboardingRequest(request(), lookups([]), { declaredLf: true }, ask),
    ).toEqual({ kind: 'lf_not_in_pcc', projectName: 'Acme' })
    expect(
      await resolveOnboardingRequest(request(), lookups([]), { treatAsNew: true }, ask),
    ).toEqual({ kind: 'non_lf_new_project', projectName: 'Acme' })
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

  it('asks "did you mean" when the match is weak', async () => {
    const weak = candidate({ name: 'Acme Labs', slug: 'acme-labs', score: 0.9 })

    const result = await resolveOnboardingRequest(request(), lookups([weak]))

    expect(result).toMatchObject({
      kind: 'needs_input',
      candidates: [weak],
      fallbackReason: 'the name only loosely matches projects in the foundation project list',
      question: {
        kind: 'did_you_mean',
        text: 'I couldn\'t find a project called "Acme". Did you mean Acme Labs?',
        options: [
          {
            id: 'yes',
            label: 'Yes, Acme Labs',
            hints: {
              pccProject: {
                projectId: 'pcc-1',
                name: 'Acme Labs',
                slug: 'acme-labs',
                isLeaf: true,
              },
            },
            suggested: true,
          },
          { id: 'no', label: 'No, ask the CDP team', handOff: true },
        ],
      },
    })
  })

  it('resolves straight to the hinted project after a "yes" answer', async () => {
    const deps = lookups([])

    const result = await resolveOnboardingRequest(request(), deps, {
      pccProject: { projectId: 'pcc-9', name: 'Acme Labs', slug: 'acme-labs', isLeaf: true },
    })

    expect(result).toEqual({
      kind: 'lf_not_in_cdp',
      pccProject: {
        projectId: 'pcc-9',
        name: 'Acme Labs',
        slug: 'acme-labs',
        score: 1,
        isLeaf: true,
      },
    })
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('treats the project as new after a "not LF" answer', async () => {
    const deps = lookups([candidate()])

    const result = await resolveOnboardingRequest(request(), deps, { treatAsNew: true })

    expect(result).toEqual({ kind: 'non_lf_new_project', projectName: 'Acme' })
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('ignores candidates below the weak threshold', async () => {
    const result = await resolveOnboardingRequest(
      request(),
      lookups([candidate({ name: 'Other', slug: 'other', score: 0.4 })]),
    )

    expect(result.kind).toBe('non_lf_new_project')
  })

  it('asks which side is right when the request contradicts a strong PCC match', async () => {
    const result = await resolveOnboardingRequest(
      request({ declaredLf: false }),
      lookups([candidate()]),
    )

    expect(result).toMatchObject({
      kind: 'needs_input',
      question: {
        kind: 'lf_conflict',
        text: "You said Acme isn't an LF project, but I found an LF project called Acme. Which is right?",
        options: [
          { id: 'use_lf', label: 'Use the LF project Acme', hints: { declaredLf: true } },
          { id: 'not_lf', label: "It's not LF, treat it as new", hints: { treatAsNew: true } },
        ],
      },
    })
  })

  it('follows a declared-LF override after the conflict answer', async () => {
    const result = await resolveOnboardingRequest(
      request({ declaredLf: false }),
      lookups([candidate()]),
      { declaredLf: true },
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('is ambiguous when the repositories can only be found in linked pages', async () => {
    const deps = lookups([candidate()])

    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: [], linksToFollow: ['https://acme.org/projects'] }),
      deps,
    )

    expect(result).toEqual({
      kind: 'ambiguous',
      reason: "repositories would have to be read from linked pages, which I don't open",
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

  it('treats a name wrapped in "the … project" as an exact match of the bare name', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'The K2s project' }),
      lookups([candidate({ name: 'K2s', slug: 'k2s', score: 0.83 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('treats the bare name as an exact match of a PCC project called "… Project"', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'Xen' }),
      lookups([candidate({ name: 'Xen Project', slug: 'xenproject', score: 0.83 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('still treats a name that differs only by separators from a "… Project" name as exact', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'Yocto-Project' }),
      lookups([candidate({ name: 'Yocto Project', slug: 'yocto', score: 0.5 })]),
    )

    expect(result.kind).toBe('lf_not_in_cdp')
  })

  it('still treats a full name equal to the slug as an exact match', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: 'Xen Project' }),
      lookups([candidate({ name: 'Xen Hypervisor', slug: 'xenproject', score: 0.83 })]),
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

  it('fails as a configuration error when the PCC lookup is not configured', async () => {
    await expect(resolveOnboardingRequest(request(), withoutPcc(lookups([])))).rejects.toThrow(
      OnboardingConfigurationError,
    )
  })

  it('skips a request whose GitHub repositories are all tracked in CDP', async () => {
    const deps = lookups([candidate()], null, {
      trackedRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'],
    })

    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] }),
      deps,
    )

    expect(result).toEqual({
      kind: 'already_tracked',
      repoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'],
    })
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('skips tracked repositories even when the requester asks about the hierarchy', async () => {
    const result = await resolveOnboardingRequest(
      request({ asksAboutHierarchy: true }),
      lookups([], null, { trackedRepoUrls: ['https://github.com/acme/one'] }),
    )

    expect(result).toEqual({
      kind: 'already_tracked',
      repoUrls: ['https://github.com/acme/one'],
    })
  })

  it('keeps classifying when only some of the repositories are tracked', async () => {
    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] }),
      lookups([], null, { trackedRepoUrls: ['https://github.com/acme/one'] }),
    )

    expect(result.kind).toBe('non_lf_new_project')
  })

  it('does not check tracked repositories for a request without GitHub repositories', async () => {
    const deps = lookups([], null, { trackedRepoUrls: [] })

    await resolveOnboardingRequest(
      request({ githubRepoUrls: [], nonGithubRepoUrls: ['https://gitlab.com/acme/tool'] }),
      deps,
    )

    expect(deps.findTrackedRepoUrls).not.toHaveBeenCalled()
  })

  it('flags an LF owner already in CDP when PCC has no match', async () => {
    const result = await resolveOnboardingRequest(
      request(),
      lookups([], null, { lfOwners: ['acme'] }),
    )

    expect(result).toEqual({ kind: 'lf_owner_in_cdp', owners: ['acme'] })
  })

  it('prefers the owner heuristic over a declared LF project missing from PCC', async () => {
    const result = await resolveOnboardingRequest(
      request({ declaredLf: true }),
      lookups([], null, { lfOwners: ['acme'] }),
    )

    expect(result.kind).toBe('lf_owner_in_cdp')
  })

  it('falls back to the owner heuristic when the project name is unknown', async () => {
    const deps = lookups([candidate()], null, { lfOwners: ['acme'] })

    const result = await resolveOnboardingRequest(request({ projectName: null }), deps)

    expect(result).toEqual({ kind: 'lf_owner_in_cdp', owners: ['acme'] })
    expect(deps.findPccCandidates).not.toHaveBeenCalled()
  })

  it('falls back to the owner heuristic when the PCC lookup is not configured', async () => {
    const result = await resolveOnboardingRequest(
      request(),
      withoutPcc(lookups([], null, { lfOwners: ['acme'] })),
    )

    expect(result).toEqual({ kind: 'lf_owner_in_cdp', owners: ['acme'] })
  })

  it('requires every owner to be exclusively LF for the owner heuristic', async () => {
    const result = await resolveOnboardingRequest(
      request({ githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/other/two'] }),
      lookups([], null, { lfOwners: ['acme'] }),
    )

    expect(result).toEqual({ kind: 'non_lf_new_project', projectName: 'Acme' })
  })

  it('asks the owner lookup once per distinct owner', async () => {
    const deps = lookups([], null)

    await resolveOnboardingRequest(
      request({ githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] }),
      deps,
    )

    expect(deps.findExclusivelyLfOwners).toHaveBeenCalledWith(['acme'])
  })

  it('skips the tracked-repo check and the owner heuristic when their lookups are absent', async () => {
    const deps = lookups([], null)

    const result = await resolveOnboardingRequest(request({ declaredLf: true }), {
      findPccCandidates: deps.findPccCandidates,
      findCdpSegmentByPccProject: deps.findCdpSegmentByPccProject,
    })

    expect(result).toEqual({ kind: 'lf_not_in_pcc', projectName: 'Acme' })
  })

  it('does not run the owner heuristic after a strong PCC match', async () => {
    const deps = lookups([candidate()], null, { lfOwners: ['acme'] })

    const result = await resolveOnboardingRequest(request(), deps)

    expect(result.kind).toBe('lf_not_in_cdp')
    expect(deps.findExclusivelyLfOwners).not.toHaveBeenCalled()
  })

  it('proposes a new project without a name when nothing identifies it as LF', async () => {
    const result = await resolveOnboardingRequest(request({ projectName: null }), lookups([]))

    expect(result).toEqual({ kind: 'non_lf_new_project', projectName: null })
  })

  it('is ambiguous when the project is declared LF but has no name', async () => {
    const result = await resolveOnboardingRequest(
      request({ projectName: null, declaredLf: true }),
      lookups([]),
    )

    expect(result).toEqual({
      kind: 'ambiguous',
      reason: "I couldn't tell which project this repository belongs to",
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

  it('asks which project when several leaf projects tie for the best score', async () => {
    const first = candidate({ projectId: 'pcc-1' })
    const second = candidate({ projectId: 'pcc-2' })
    const deps = lookups([first, second])

    const result = await resolveOnboardingRequest(request(), deps)

    expect(result).toMatchObject({
      kind: 'needs_input',
      candidates: [first, second],
      question: {
        kind: 'pick_project',
        options: [
          { id: 'pcc-1', label: 'Acme (acme)' },
          { id: 'pcc-2', label: 'Acme (acme)' },
          { id: 'not_sure', handOff: true },
        ],
      },
    })
    expect(deps.findCdpSegmentByPccProject).not.toHaveBeenCalled()
  })

  it('prefers the leaf project over a parent with the same name', async () => {
    const parent = candidate({ projectId: 'pcc-parent', isLeaf: false })
    const leaf = candidate({ projectId: 'pcc-leaf' })

    const result = await resolveOnboardingRequest(request(), lookups([parent, leaf]))

    expect(result).toEqual({ kind: 'lf_not_in_cdp', pccProject: leaf })
  })

  it('is ambiguous when the best match is a PCC parent project without a project of its own', async () => {
    const parent = candidate({ isLeaf: false })
    const deps = lookups([parent])

    const result = await resolveOnboardingRequest(request(), deps)

    expect(result).toMatchObject({ kind: 'ambiguous', candidates: [parent] })
    expect(deps.findCdpSegmentByPccProject).toHaveBeenCalledWith(parent.projectId)
  })

  it('treats a PCC parent project with its own LFX Insights project as that project', async () => {
    const parent = candidate({ isLeaf: false })
    const own = segment()

    const result = await resolveOnboardingRequest(request(), lookups([parent], own))

    expect(result).toEqual({
      kind: 'lf_in_cdp',
      pccProject: parent,
      segment: own,
      action: 'create_integration',
    })
  })

  it('asks about the LF conflict before giving up on a parent project with a project of its own', async () => {
    const parent = candidate({ isLeaf: false })

    const result = await resolveOnboardingRequest(
      request({ declaredLf: false }),
      lookups([parent], segment()),
    )

    expect(result).toMatchObject({ kind: 'needs_input', question: { kind: 'lf_conflict' } })
  })

  it('prefers a separator-insensitive exact match over a closer scoring name', async () => {
    const exact = candidate({
      projectId: 'pcc-exact',
      name: 'c++-tools',
      slug: 'cpp-tools',
      score: 0.95,
    })
    const closer = candidate({
      projectId: 'pcc-closer',
      name: 'c++ tool',
      slug: 'c++-tool',
      score: 0.98,
    })

    const result = await resolveOnboardingRequest(
      request({ projectName: 'C++ Tools' }),
      lookups([closer, exact]),
    )

    expect(result).toEqual({ kind: 'lf_not_in_cdp', pccProject: exact })
  })

  it('is ambiguous when several leaf projects match the name exactly with different scores', async () => {
    const first = candidate({ projectId: 'pcc-1', name: 'Acme', slug: 'acme', score: 1 })
    const second = candidate({ projectId: 'pcc-2', name: 'ACME', slug: 'acme-two', score: 0.9 })

    const result = await resolveOnboardingRequest(request(), lookups([first, second]))

    expect(result).toMatchObject({ kind: 'needs_input', candidates: [first, second] })
  })

  it('asks which segment when several CDP segments share the PCC project id', async () => {
    const pccProject = candidate()
    const duplicates = [
      segment({ segmentId: 'seg-1', name: 'Acme' }),
      segment({ segmentId: 'seg-2', name: 'Acme (LF Edge)' }),
    ]

    const result = await resolveOnboardingRequest(request(), lookups([pccProject], { duplicates }))

    expect(result).toMatchObject({
      kind: 'needs_input',
      fallbackReason: 'more than one LFX Insights project shares this foundation project',
      candidates: [pccProject],
      question: {
        kind: 'pick_project',
        text: 'There are 2 Acme projects in LFX Insights. Which one should this repo go in?',
        options: [
          { id: 'seg-1', label: 'Acme', hints: { segmentId: 'seg-1' } },
          { id: 'seg-2', label: 'Acme (LF Edge)', hints: { segmentId: 'seg-2' } },
          { id: 'not_sure', handOff: true },
        ],
      },
    })
  })

  it('tells duplicate segments with the same name apart by their repositories', async () => {
    const pccProject = candidate({ name: 'FIDOPower' })
    const duplicates = [
      segment({ segmentId: 'seg-1', name: 'FIDOPower', slug: 'fidopower', repositoryCount: 28 }),
      segment({ segmentId: 'seg-2', name: 'FIDOPower', slug: 'openfido', repositoryCount: 0 }),
    ]

    const result = await resolveOnboardingRequest(request(), lookups([pccProject], { duplicates }))

    expect(result).toMatchObject({
      question: {
        options: [
          { id: 'seg-1', label: 'FIDOPower · 28 repos' },
          { id: 'seg-2', label: 'FIDOPower · no repos yet' },
          { id: 'not_sure' },
        ],
      },
    })
  })

  it('falls back to the slug when name and repositories are the same', async () => {
    const pccProject = candidate({ name: 'Rune DSL' })
    const duplicates = [
      segment({ segmentId: 'seg-1', name: 'Rune DSL', slug: 'rosetta-dsl', repositoryCount: 1 }),
      segment({ segmentId: 'seg-2', name: 'Rune DSL', slug: 'rune-dsl', repositoryCount: 1 }),
    ]

    const result = await resolveOnboardingRequest(request(), lookups([pccProject], { duplicates }))

    expect(result).toMatchObject({
      question: {
        options: [
          { id: 'seg-1', label: 'Rune DSL (rosetta-dsl) · 1 repo' },
          { id: 'seg-2', label: 'Rune DSL (rune-dsl) · 1 repo' },
          { id: 'not_sure' },
        ],
      },
    })
  })

  it('keeps the distinguishing part of long duplicate labels within the button limit', async () => {
    const name = 'Working Group for Really Long Project Names That Do Not Fit In A Button At All'
    const pccProject = candidate({ name })
    const duplicates = [
      segment({ segmentId: 'seg-1', name, slug: 'wg-long', repositoryCount: 19 }),
      segment({ segmentId: 'seg-2', name, slug: 'wg-long', repositoryCount: 0 }),
    ]

    const result = await resolveOnboardingRequest(request(), lookups([pccProject], { duplicates }))

    const labels = (result as { question: { options: { label: string }[] } }).question.options.map(
      (option) => option.label,
    )
    expect(labels[0]).toHaveLength(75)
    expect(labels[0]).toMatch(/… · 19 repos$/)
    expect(labels[1]).toMatch(/… · no repos yet$/)
    expect(new Set(labels).size).toBe(labels.length)
  })

  it('uses the chosen segment after the duplicate-segment answer', async () => {
    const pccProject = candidate()
    const duplicates = [
      segment({ segmentId: 'seg-1', name: 'Acme' }),
      segment({ segmentId: 'seg-2', name: 'Acme (LF Edge)', integration: 'github-nango' }),
    ]

    const result = await resolveOnboardingRequest(
      request(),
      lookups([pccProject], { duplicates }),
      {
        segmentId: 'seg-2',
      },
    )

    expect(result).toMatchObject({
      kind: 'lf_in_cdp',
      segment: { segmentId: 'seg-2' },
      action: 'update_integration',
    })
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

describe('coreProjectName', () => {
  it('lowercases, trims and drops a leading "the" and a trailing "project"', () => {
    expect(coreProjectName('  The K2s Project ')).toBe('k2s')
    expect(coreProjectName('Yocto Project')).toBe('yocto')
    expect(coreProjectName('OpenJS Foundation')).toBe('openjs foundation')
  })

  it('keeps the name when nothing would be left', () => {
    expect(coreProjectName('Project')).toBe('project')
    expect(coreProjectName('The')).toBe('the')
  })
})
