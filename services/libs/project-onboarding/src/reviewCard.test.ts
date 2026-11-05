import { describe, expect, it } from 'vitest'

import {
  NO_REVIEW_CARD_EXTRAS,
  resolveReviewCase,
  resolveReviewCdpUrl,
  toReviewRepoContext,
  toReviewRequestCard,
} from './reviewCard'

const row = {
  id: 'catalog-1',
  repoName: 'kops',
  repoUrl: 'https://github.com/kubernetes/kops',
  projectName: 'Kubernetes',
  sourceUrl: 'https://lfx.slack.com/archives/C123/p1700000000000100',
  classificationKind: 'lf_in_cdp' as const,
  proposedAction: 'update_integration' as const,
  matchedProjectName: 'Kubernetes',
  matchedSegmentId: 'seg-1',
  evaluationReason: null,
  evaluationResult: null,
}

describe('toReviewRequestCard', () => {
  it('maps a catalog row to a card without enum values in the visible fields', () => {
    expect(toReviewRequestCard(row, NO_REVIEW_CARD_EXTRAS)).toEqual({
      id: 'catalog-1',
      repoName: 'kops',
      repoUrl: 'https://github.com/kubernetes/kops',
      sourceUrl: 'https://lfx.slack.com/archives/C123/p1700000000000100',
      organization: null,
      repoContext: null,
      requestedName: 'Kubernetes',
      matchedName: 'Kubernetes',
      insightsState: 'Project exists with GitHub connected',
      whatsNeeded: "Add this repo to Kubernetes's GitHub connection",
      mismatch: false,
      action: 'add_to_connection',
      evaluationNote: null,
      autoApplyHold: true,
      cdpUrl: null,
      details: 'Details: lf_in_cdp · update_integration',
    })
  })

  it('carries the organization under review when the row leads one', () => {
    const organization = { owner: 'kubernetes', repositories: 12 }

    expect(toReviewRequestCard(row, NO_REVIEW_CARD_EXTRAS, organization)).toMatchObject({
      organization,
      action: 'add_to_connection',
      whatsNeeded: "Add these 12 repos to Kubernetes's GitHub connection",
    })
  })

  it('builds a new-project card with neutral creation copy and the automatic check note', () => {
    const card = toReviewRequestCard(
      {
        ...row,
        projectName: 'Acme',
        classificationKind: 'non_lf_new_project',
        proposedAction: 'create_segment',
        matchedProjectName: null,
        matchedSegmentId: null,
        evaluationReason: 'Looks like an active open source project.',
        evaluationResult: 'true',
      },
      NO_REVIEW_CARD_EXTRAS,
    )

    expect(card).toMatchObject({
      matchedName: null,
      mismatch: false,
      insightsState: 'Not in LFX Insights yet',
      whatsNeeded: 'Create a new non-LF project in LFX Insights and add this repo',
      action: 'create_project',
      autoApplyHold: true,
    })
    expect(
      toReviewRequestCard(
        {
          ...row,
          classificationKind: 'non_lf_new_project',
          proposedAction: 'create_segment',
          evaluationResult: 'false',
          evaluationReason: 'mixed signals',
        },
        NO_REVIEW_CARD_EXTRAS,
      ),
    ).toMatchObject({ action: 'create_project', autoApplyHold: false })
    expect(
      toReviewRequestCard({ ...row, proposedAction: 'create_segment' }, NO_REVIEW_CARD_EXTRAS),
    ).toMatchObject({ action: 'create_project', autoApplyHold: false })
  })

  it('flags a requested name that differs from the matched project', () => {
    const card = toReviewRequestCard(
      { ...row, projectName: 'Common Cloud Controls' },
      NO_REVIEW_CARD_EXTRAS,
    )

    expect(card.mismatch).toBe(true)
    expect(card.requestedName).toBe('Common Cloud Controls')
    expect(card.matchedName).toBe('Kubernetes')
  })

  it('ignores case and punctuation when comparing names and skips missing ones', () => {
    expect(
      toReviewRequestCard({ ...row, projectName: 'kubernetes!' }, NO_REVIEW_CARD_EXTRAS).mismatch,
    ).toBe(false)
    expect(
      toReviewRequestCard({ ...row, projectName: 'The Kubernetes project' }, NO_REVIEW_CARD_EXTRAS)
        .mismatch,
    ).toBe(false)
    expect(
      toReviewRequestCard(
        { ...row, projectName: 'Kubernetes-Project', matchedProjectName: 'Kubernetes Project' },
        NO_REVIEW_CARD_EXTRAS,
      ).mismatch,
    ).toBe(false)
    expect(toReviewRequestCard({ ...row, projectName: null }, NO_REVIEW_CARD_EXTRAS).mismatch).toBe(
      false,
    )
    expect(
      toReviewRequestCard(
        { ...row, matchedProjectName: null, matchedSegmentId: null },
        NO_REVIEW_CARD_EXTRAS,
      ).mismatch,
    ).toBe(false)
  })

  it('builds a manual card with the CDP link for rows that need a person', () => {
    const card = toReviewRequestCard(
      { ...row, proposedAction: 'human_review' },
      { cdpUrl: 'https://cdp.test/integrations/gp-1/seg-1', repoContext: null },
    )

    expect(card).toMatchObject({
      autoApplyHold: false,
      insightsState: 'Uses the old GitHub connection',
      whatsNeeded: 'Add the repo by hand in CDP, then mark as done',
      action: 'manual',
      cdpUrl: 'https://cdp.test/integrations/gp-1/seg-1',
    })
  })
})

describe('toReviewRepoContext', () => {
  it('keeps only what the card shows', () => {
    expect(
      toReviewRepoContext({
        description: 'A repo',
        primaryLanguage: 'Go',
        stars: 10,
        forks: 2,
        openIssues: 1,
        closedIssues: 5,
        hasIssuesEnabled: true,
        openPullRequests: 1,
        closedPullRequests: 5,
        pushedAt: '2026-01-01T00:00:00Z',
        createdAt: '2020-01-01T00:00:00Z',
        isArchived: true,
        isFork: true,
        parentFullName: 'upstream/repo',
      }),
    ).toEqual({
      description: 'A repo',
      stars: 10,
      pushedAt: '2026-01-01T00:00:00Z',
      isArchived: true,
      isFork: true,
      parentFullName: 'upstream/repo',
    })
  })
})

describe('resolveReviewCase', () => {
  it('picks the case from the proposed action, the kind and the matched segment', () => {
    expect(resolveReviewCase(row)).toBe('add_to_connection')
    expect(resolveReviewCase({ ...row, proposedAction: 'create_segment' })).toBe('new_project')
    expect(resolveReviewCase({ ...row, proposedAction: 'create_integration' })).toBe(
      'connect_github',
    )
    expect(resolveReviewCase({ ...row, proposedAction: 'human_review' })).toBe('legacy_connection')
    expect(
      resolveReviewCase({
        ...row,
        classificationKind: 'lf_not_in_cdp',
        proposedAction: 'human_review',
        matchedSegmentId: null,
      }),
    ).toBe('project_not_set_up')
    expect(
      resolveReviewCase({
        ...row,
        classificationKind: 'lf_not_in_pcc',
        proposedAction: 'human_review',
        matchedSegmentId: null,
      }),
    ).toBe('not_in_pcc')
    expect(
      resolveReviewCase({
        ...row,
        classificationKind: 'non_lf_new_project',
        proposedAction: 'human_review',
        matchedSegmentId: null,
      }),
    ).toBe('new_project_by_hand')
    expect(
      resolveReviewCase({ ...row, proposedAction: 'human_review', matchedSegmentId: null }),
    ).toBe('owner_only_match')
    expect(
      resolveReviewCase({
        ...row,
        classificationKind: 'ambiguous',
        proposedAction: 'human_review',
        matchedSegmentId: null,
      }),
    ).toBe('handed_off')
    expect(resolveReviewCase({ ...row, proposedAction: null })).toBe('unknown')
  })
})

describe('resolveReviewCdpUrl', () => {
  const manual = { proposedAction: 'human_review' as const, matchedSegmentId: 'seg-1' }

  function qxWithSegments(rows: { id: string; grandparentId: string | null }[]) {
    return { select: async () => rows } as never
  }

  it('returns nothing without a frontend url or for rows handled automatically', async () => {
    expect(await resolveReviewCdpUrl(qxWithSegments([]), manual, undefined)).toBeNull()
    expect(
      await resolveReviewCdpUrl(
        qxWithSegments([]),
        { ...manual, proposedAction: 'update_integration' },
        'https://cdp.test',
      ),
    ).toBeNull()
  })

  it('links the matched sub-project page when its project group is known', async () => {
    expect(
      await resolveReviewCdpUrl(
        qxWithSegments([{ id: 'seg-1', grandparentId: 'gp-1' }]),
        manual,
        'https://cdp.test/',
      ),
    ).toBe('https://cdp.test/integrations/gp-1/seg-1')
  })

  it('falls back to the admin page without a matched segment or project group', async () => {
    expect(
      await resolveReviewCdpUrl(
        qxWithSegments([]),
        { ...manual, matchedSegmentId: null },
        'https://cdp.test',
      ),
    ).toBe('https://cdp.test/admin#project-groups')
    expect(
      await resolveReviewCdpUrl(
        qxWithSegments([{ id: 'seg-1', grandparentId: null }]),
        manual,
        'https://cdp.test',
      ),
    ).toBe('https://cdp.test/admin#project-groups')
  })
})
