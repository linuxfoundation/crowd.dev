import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/data-access-layer', () => ({ lockProjectCatalogOnboardingTarget: vi.fn() }))

import { lockProjectCatalogOnboardingTarget } from '@crowd/data-access-layer'

import {
  IProposedActionDeps,
  runOrganizationProposedAction,
  runOrganizationProposedActionSerialized,
  runProposedAction,
  runProposedActionSerialized,
} from './proposedAction'

const onboarded = { outcome: 'onboarded' as const, segmentId: 'seg', error: null }

const deps: IProposedActionDeps = {
  findCdpSegmentByPccProject: vi.fn(),
  onboardProject: vi.fn(async () => onboarded),
  onboardOrganizationProject: vi.fn(async () => onboarded),
  connectReposToSegment: vi.fn(async () => onboarded),
}

const project = {
  id: 'row-1',
  repoUrl: 'https://github.com/kubernetes/kops',
  repoName: 'kops',
  projectName: null,
  projectSlug: 'kubernetes-kops',
  proposedAction: null,
  pccProjectId: null,
  matchedSegmentId: null,
  requestHints: null,
}

describe('runProposedAction', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      segmentId: 'seg',
      name: 'Kubernetes',
      slug: 'kubernetes',
      integration: 'none',
    })
  })

  it.each([null, 'create_segment' as const])(
    'creates a non-LF segment and integration for proposedAction %s',
    async (proposedAction) => {
      const result = await runProposedAction({ ...project, proposedAction }, deps)

      expect(result).toEqual(onboarded)
      expect(deps.onboardProject).toHaveBeenCalledWith({
        id: 'row-1',
        repoUrl: project.repoUrl,
        repoName: 'kops',
        projectSlug: 'kubernetes-kops',
      })
      expect(deps.connectReposToSegment).not.toHaveBeenCalled()
    },
  )

  it.each(['create_integration' as const, 'update_integration' as const])(
    'connects the repo to the CDP segment of the PCC project for proposedAction %s',
    async (proposedAction) => {
      const result = await runProposedAction(
        { ...project, proposedAction, pccProjectId: 'pcc-1' },
        deps,
      )

      expect(result).toEqual(onboarded)
      expect(deps.findCdpSegmentByPccProject).toHaveBeenCalledWith('pcc-1')
      expect(deps.connectReposToSegment).toHaveBeenCalledWith({
        segmentId: 'seg',
        repoUrls: [project.repoUrl],
      })
      expect(deps.onboardProject).not.toHaveBeenCalled()
    },
  )

  it('fails when an LF action has no PCC project id', async () => {
    const result = await runProposedAction(
      { ...project, proposedAction: 'create_integration' },
      deps,
    )

    expect(result.outcome).toBe('error')
    expect(result.error).toContain('requires a PCC project id')
    expect(deps.connectReposToSegment).not.toHaveBeenCalled()
  })

  it('fails when the PCC project has no CDP segment', async () => {
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue(null)

    const result = await runProposedAction(
      { ...project, proposedAction: 'create_integration', pccProjectId: 'pcc-1' },
      deps,
    )

    expect(result.outcome).toBe('error')
    expect(result.error).toContain('No CDP segment found')
  })

  it('fails when several CDP segments share the PCC project', async () => {
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      duplicates: [
        { segmentId: 'a', name: 'A', slug: 'a', integration: 'none' },
        { segmentId: 'b', name: 'B', slug: 'b', integration: 'none' },
      ],
    })

    const result = await runProposedAction(
      { ...project, proposedAction: 'update_integration', pccProjectId: 'pcc-1' },
      deps,
    )

    expect(result.outcome).toBe('error')
    expect(result.error).toContain('Several CDP segments')
  })

  it('uses the segment the requester picked among duplicates', async () => {
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      duplicates: [
        { segmentId: 'a', name: 'A', slug: 'a', integration: 'none' },
        { segmentId: 'b', name: 'B', slug: 'b', integration: 'github-nango' },
      ],
    })

    const result = await runProposedAction(
      {
        ...project,
        proposedAction: 'update_integration',
        pccProjectId: 'pcc-1',
        matchedSegmentId: 'b',
      },
      deps,
    )

    expect(result).toBe(onboarded)
    expect(deps.connectReposToSegment).toHaveBeenCalledWith({
      segmentId: 'b',
      repoUrls: ['https://github.com/kubernetes/kops'],
    })
  })

  it('still fails when the picked segment is no longer among the duplicates', async () => {
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      duplicates: [
        { segmentId: 'a', name: 'A', slug: 'a', integration: 'none' },
        { segmentId: 'b', name: 'B', slug: 'b', integration: 'none' },
      ],
    })

    const result = await runProposedAction(
      {
        ...project,
        proposedAction: 'update_integration',
        pccProjectId: 'pcc-1',
        matchedSegmentId: 'gone',
      },
      deps,
    )

    expect(result.outcome).toBe('error')
    expect(result.error).toContain('Several CDP segments')
  })

  it('fails when the segment has a legacy GitHub integration', async () => {
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      segmentId: 'seg',
      name: 'Kubernetes',
      slug: 'kubernetes',
      integration: 'github-v1',
    })

    const result = await runProposedAction(
      { ...project, proposedAction: 'update_integration', pccProjectId: 'pcc-1' },
      deps,
    )

    expect(result.outcome).toBe('error')
    expect(result.error).toContain('legacy GitHub integration')
    expect(deps.connectReposToSegment).not.toHaveBeenCalled()
  })

  it('never onboards a human_review row', async () => {
    const result = await runProposedAction({ ...project, proposedAction: 'human_review' }, deps)

    expect(result.outcome).toBe('error')
    expect(deps.onboardProject).not.toHaveBeenCalled()
    expect(deps.connectReposToSegment).not.toHaveBeenCalled()
  })
})

describe('runOrganizationProposedAction', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      segmentId: 'seg',
      name: 'Kubernetes',
      slug: 'kubernetes',
      integration: 'github-nango',
    })
  })

  it('connects every repository of the organization to the segment in one call', async () => {
    const rows = ['one', 'two', 'three'].map((name) => ({
      ...project,
      id: name,
      repoUrl: `https://github.com/kubernetes/${name}`,
      proposedAction: 'update_integration' as const,
      pccProjectId: 'pcc-1',
    }))

    const result = await runOrganizationProposedAction(rows, deps)

    expect(result).toEqual(onboarded)
    expect(deps.findCdpSegmentByPccProject).toHaveBeenCalledTimes(1)
    expect(deps.connectReposToSegment).toHaveBeenCalledWith({
      segmentId: 'seg',
      repoUrls: rows.map((row) => row.repoUrl),
    })
  })

  it('creates one project with every repository of a new organization', async () => {
    const rows = ['one', 'two'].map((name) => ({
      ...project,
      id: name,
      repoUrl: `https://github.com/kubernetes/${name}`,
      projectName: 'Kube Tools',
      proposedAction: 'create_segment' as const,
    }))

    const result = await runOrganizationProposedAction(rows, deps)

    expect(result).toEqual(onboarded)
    expect(deps.onboardOrganizationProject).toHaveBeenCalledWith({
      id: 'one',
      projectName: 'Kube Tools',
      repoUrls: ['https://github.com/kubernetes/one', 'https://github.com/kubernetes/two'],
    })
    expect(deps.onboardProject).not.toHaveBeenCalled()
    expect(deps.connectReposToSegment).not.toHaveBeenCalled()
  })

  it('refuses a mixed group and takes the segment lock when it writes one', async () => {
    const empty = await runOrganizationProposedAction([], deps)
    expect(empty.outcome).toBe('error')
    const mixed = await runOrganizationProposedAction(
      [
        { ...project, proposedAction: 'update_integration', pccProjectId: 'pcc-1' },
        { ...project, id: 'two', proposedAction: 'create_segment' },
      ],
      deps,
    )
    expect(mixed.outcome).toBe('error')
    expect(deps.connectReposToSegment).not.toHaveBeenCalled()
    expect(deps.onboardOrganizationProject).not.toHaveBeenCalled()

    const tx = { marker: 'tx' }
    const qx = { tx: async (run: (inner: unknown) => Promise<unknown>) => run(tx) } as never
    const buildDeps = vi.fn(() => deps)
    const rows = [
      { ...project, proposedAction: 'create_integration' as const, pccProjectId: 'pcc-1' },
    ]

    await runOrganizationProposedActionSerialized(rows, qx, buildDeps)

    expect(lockProjectCatalogOnboardingTarget).toHaveBeenCalledWith(tx, 'pcc-1')
    expect(buildDeps).toHaveBeenCalledWith(tx)
  })
})

describe('runProposedActionSerialized', () => {
  const txQx = { marker: 'tx' } as never
  const qx = { tx: vi.fn(async (fn: (tx: unknown) => Promise<unknown>) => fn(txQx)) } as never
  const buildDeps = vi.fn(() => deps)

  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(deps.findCdpSegmentByPccProject).mockResolvedValue({
      segmentId: 'seg',
      name: 'Kubernetes',
      slug: 'kubernetes',
      integration: 'none',
    })
  })

  it.each(['create_integration' as const, 'update_integration' as const])(
    'locks the PCC project inside a transaction for proposedAction %s',
    async (proposedAction) => {
      const result = await runProposedActionSerialized(
        { ...project, proposedAction, pccProjectId: 'pcc-1' },
        qx,
        buildDeps,
      )

      expect(result).toEqual(onboarded)
      expect(lockProjectCatalogOnboardingTarget).toHaveBeenCalledWith(txQx, 'pcc-1')
      expect(lockProjectCatalogOnboardingTarget).toHaveBeenCalledBefore(
        vi.mocked(deps.connectReposToSegment),
      )
      expect(buildDeps).toHaveBeenCalledWith(txQx)
    },
  )

  it('runs a segment creation without any lock', async () => {
    const result = await runProposedActionSerialized(
      { ...project, proposedAction: 'create_segment' },
      qx,
      buildDeps,
    )

    expect(result).toEqual(onboarded)
    expect(lockProjectCatalogOnboardingTarget).not.toHaveBeenCalled()
    expect(buildDeps).toHaveBeenCalledWith(qx)
  })

  it('serializes the creation of an organization project on the organization', async () => {
    const tx = { marker: 'tx' }
    const lockingQx = { tx: async (run: (inner: unknown) => Promise<unknown>) => run(tx) } as never
    const rows = [
      {
        ...project,
        proposedAction: 'create_segment' as const,
        requestHints: { organization: 'Kubernetes' },
      },
    ]

    await runOrganizationProposedActionSerialized(rows, lockingQx, buildDeps)

    expect(lockProjectCatalogOnboardingTarget).toHaveBeenCalledWith(tx, 'organization:kubernetes')
    expect(buildDeps).toHaveBeenCalledWith(tx)
  })
})
