import { describe, expect, it } from 'vitest'

import {
  buildBotErroredReply,
  buildBotEvaluationErrorContent,
  buildBotOnboardedContent,
  buildBotOnboardedReply,
  buildBotDoneReply,
  buildBotRejectedReply,
  buildBotReviewReply,
  buildBotSkippedReply,
  buildInsightsProjectUrl,
} from './botThreadReply'

const project = {
  repoUrl: 'https://github.com/acme/widget',
  projectSlug: 'acme-widget',
  proposedAction: null,
  projectName: 'Acme',
  classificationKind: null,
  matchedSegmentId: null,
}

const REPO_LABEL = '*<https://github.com/acme/widget|acme/widget>*'

describe('botThreadReply', () => {
  it('links the insights project when a segment was created', () => {
    const reply = buildBotOnboardedReply(project)

    expect(reply).toContain(`<${buildInsightsProjectUrl('acme-widget')}|View project>`)
    expect(reply).toContain('✅')
    expect(buildBotOnboardedReply({ ...project, proposedAction: 'create_segment' })).toContain(
      'https://insights.linuxfoundation.org/project/',
    )
  })

  it('names the project, the approver and links it when the insights project is known', () => {
    const content = buildBotOnboardedContent(
      { ...project, proposedAction: 'update_integration', reviewedBy: 'U123' },
      { name: 'Acme & Co', slug: 'acme-co' },
    )

    expect(content.text).toBe(
      `✅ ${REPO_LABEL} is now in LFX Insights under *Acme &amp; Co*. Approved by <@U123>. Activity shows up after the first data sync.`,
    )
    expect(content.blocks).toMatchObject([
      { type: 'section' },
      { type: 'context', elements: [{ text: 'Activity shows up after the first data sync.' }] },
      {
        type: 'actions',
        elements: [
          {
            action_id: 'view_insights_project:0',
            url: 'https://insights.linuxfoundation.org/project/acme-co',
            text: { text: 'View Acme & Co in LFX Insights' },
          },
        ],
      },
    ])
  })

  it('omits the approver line for rows nobody reviewed and falls back without a project', () => {
    const auto = buildBotOnboardedContent(
      { ...project, proposedAction: 'create_segment', reviewedBy: null },
      { name: 'Acme', slug: 'acme' },
    )
    expect(auto.text).not.toContain('Approved by')

    const fallback = buildBotOnboardedContent(
      { ...project, proposedAction: 'update_integration', reviewedBy: 'U123' },
      null,
    )
    expect(fallback).toEqual({
      text: buildBotOnboardedReply({ ...project, proposedAction: 'update_integration' }),
    })
  })

  it('reports the join to the existing project for integration actions without naming it', () => {
    for (const proposedAction of ['create_integration', 'update_integration'] as const) {
      const reply = buildBotOnboardedReply({ ...project, proposedAction })
      expect(reply).toContain(`${REPO_LABEL} is now in LFX Insights, in its existing project.`)
      expect(reply).not.toContain('Acme')
      expect(reply).not.toContain('insights.linuxfoundation.org')
    }
  })

  it('renders the repository as a linked owner/repo label', () => {
    expect(buildBotSkippedReply(project, 'it is already in LFX Insights')).toBe(
      [
        `🚫 ${REPO_LABEL} wasn't added: it is already in LFX Insights.`,
        'Reply here mentioning me with the project name and the GitHub links, or ask the CDP team in this channel.',
      ].join('\n'),
    )
  })

  it('speaks for the whole organization when its evaluation skipped it', () => {
    expect(
      buildBotSkippedReply(project, 'it looks like a personal sandbox', {
        owner: 'acme',
        repositories: 3,
      }),
    ).toContain(
      "🚫 *github.com/acme* (3 repositories) weren't added: it looks like a personal sandbox.",
    )
  })

  it('truncates long reasons, drops trailing periods and escapes mrkdwn control characters', () => {
    const reply = buildBotSkippedReply(
      { repoUrl: 'https://gitlab.com/acme/<widget>', classificationKind: 'ambiguous' },
      `a & b ${'x'.repeat(600)}.`,
    )

    expect(reply).toContain('*https://gitlab.com/acme/&lt;widget&gt;*')
    expect(reply).toContain('a &amp; b')
    expect(reply).toContain('…')
    expect(reply).not.toContain('..')
    expect(reply.length).toBeLessThan(750)
  })

  it('tells the requester a new organization project is created by hand', () => {
    expect(
      buildBotReviewReply(
        {
          ...project,
          classificationKind: 'non_lf_new_project',
          proposedAction: 'human_review',
          reviewReason:
            "Acme isn't in LFX Insights yet; a new non-LF project with the 2 repositories of github.com/acme is created by the CDP team",
        },
        { owner: 'acme', repositories: 2 },
      ),
    ).toContain(
      "I've asked the CDP team to create it and add the repositories; they'll let you know here.",
    )
  })

  it('speaks of the whole organization when the row leads one', () => {
    const organization = { owner: 'acme', repositories: 12 }
    const org = '*github.com/acme* (12 repositories)'

    expect(
      buildBotReviewReply(
        { ...project, proposedAction: 'update_integration', reviewReason: null },
        organization,
      ),
    ).toBe(
      `👀 ${org} need a quick check before I add them. The CDP team will approve or reject it here, and I'll post the result.`,
    )
    expect(
      buildBotRejectedReply(
        { ...project, reviewedBy: 'U123', decisionReason: null, decisionNote: null },
        organization,
      ),
    ).toContain(`🚫 ${org} weren't added. <@U123> reviewed it and rejected the request.`)
    expect(buildBotDoneReply({ ...project, reviewedBy: 'U123' }, organization)).toContain(
      `✅ ${org} were added to LFX Insights by hand by <@U123>.`,
    )
  })

  it('reports an already tracked repository as a success without a next step', () => {
    expect(
      buildBotSkippedReply(
        { ...project, classificationKind: 'already_tracked' },
        'it is already in LFX Insights',
      ),
    ).toBe(`📘 ${REPO_LABEL} is already in LFX Insights. Nothing else to do.`)
  })

  it('tells the requester the CDP team has the request, in one human sentence per case', () => {
    expect(buildBotReviewReply({ ...project, reviewReason: null })).toBe(
      `👀 ${REPO_LABEL} needs a quick check before I add it. I've asked the CDP team to pick the right project and add it by hand; they'll confirm here.`,
    )
    expect(
      buildBotReviewReply({
        ...project,
        classificationKind: 'ambiguous',
        proposedAction: 'human_review',
        reviewReason: "the requester said Acme Widgets isn't Acme",
      }),
    ).toBe(
      `👀 ${REPO_LABEL}: the requester said Acme Widgets isn't Acme. I've asked the CDP team to pick the right project and add it by hand; they'll confirm here.`,
    )
    expect(
      buildBotReviewReply({
        ...project,
        proposedAction: 'update_integration',
        reviewReason: 'Acme is in LFX Insights, but this repo needs a <quick> check',
      }),
    ).toBe(
      `👀 ${REPO_LABEL}: Acme is in LFX Insights, but this repo needs a &lt;quick&gt; check. The CDP team will approve or reject it here, and I'll post the result.`,
    )
    expect(
      buildBotReviewReply({
        ...project,
        classificationKind: 'lf_in_cdp',
        proposedAction: 'human_review',
        matchedSegmentId: 'seg-1',
        reviewReason:
          "Acme still uses an older GitHub connection, so I can't add repositories to it myself",
      }),
    ).toBe(
      `👀 ${REPO_LABEL}: Acme still uses an older GitHub connection, so I can't add repositories to it myself. I've asked the CDP team to add it by hand; they'll mark it done here once it's in.`,
    )
    expect(
      buildBotReviewReply({
        ...project,
        classificationKind: 'lf_not_in_cdp',
        proposedAction: 'human_review',
        reviewReason: "Acme is an LF project, but it isn't set up in LFX Insights yet",
      }),
    ).toContain("I've asked the CDP team to create it and add the repo; they'll let you know here.")
    expect(
      buildBotReviewReply({
        ...project,
        classificationKind: 'lf_not_in_pcc',
        proposedAction: 'human_review',
        matchedSegmentId: null,
        reviewReason:
          "Acme is described as an LF project, but it isn't in the foundation project list yet",
      }),
    ).toBe(
      `👀 ${REPO_LABEL}: Acme is described as an LF project, but it isn't in the foundation project list yet. I've asked the CDP team to check it with PCC and add it; they'll let you know here.`,
    )
    expect(
      buildBotReviewReply({
        ...project,
        classificationKind: 'lf_in_cdp',
        proposedAction: 'human_review',
        reviewReason:
          "the GitHub owner acme only has LF projects in LFX Insights, so I couldn't tell which one this repo belongs to",
      }),
    ).toContain(
      "I've asked the CDP team to pick the right project and add it by hand; they'll confirm here.",
    )
  })

  it('tells the requester when the repo was added by hand', () => {
    expect(buildBotDoneReply({ ...project, reviewedBy: 'U123' })).toBe(
      `✅ ${REPO_LABEL} was added to LFX Insights by hand by <@U123>. Activity shows up after the first data sync.`,
    )
    expect(buildBotDoneReply({ ...project, reviewedBy: null })).toContain(
      'by hand by the CDP team.',
    )
  })

  it('tells the requester a new project was sent to the CDP team', () => {
    expect(
      buildBotReviewReply({
        ...project,
        proposedAction: 'create_segment',
        reviewReason:
          "Acme isn't in LFX Insights yet; it would be created as a non-LF project, and new projects are added by the CDP team",
      }),
    ).toBe(
      `👀 ${REPO_LABEL}: Acme isn't in LFX Insights yet; it would be created as a non-LF project, and new projects are added by the CDP team. I've sent your request to the CDP team; they'll approve or reject it here.`,
    )
  })

  it('tells the requester a failed check was saved and offers to try again', () => {
    const content = buildBotEvaluationErrorContent({ ...project, id: 'catalog-1' })

    expect(content.text).toBe(
      `⚠️ Something went wrong on our side while checking ${REPO_LABEL}. Your request is saved and the CDP team has been alerted.`,
    )
    expect(content.blocks).toMatchObject([
      { type: 'section' },
      {
        type: 'actions',
        elements: [
          { action_id: 'retry_evaluation', value: 'catalog-1', text: { text: 'Try again' } },
        ],
      },
    ])
  })

  it('explains errors without raw errors', () => {
    const errored = buildBotErroredReply(project)
    expect(errored).toContain('⚠️ Something went wrong on our side')
    expect(errored).toContain('ask the CDP team'.replace('ask', 'Ask'))
  })

  it('mentions the reviewer who rejected the request', () => {
    const rejected = { ...project, reviewedBy: 'U123', decisionReason: null, decisionNote: null }
    expect(buildBotRejectedReply(rejected)).toBe(
      [
        `🚫 ${REPO_LABEL} wasn't added. <@U123> reviewed it and rejected the request.`,
        'Reply here if this looks wrong.',
      ].join('\n'),
    )
    expect(buildBotRejectedReply({ ...rejected, reviewedBy: null })).toContain(
      'The CDP team reviewed it',
    )
  })

  it('passes the rejection reason and the quoted note to the requester', () => {
    expect(
      buildBotRejectedReply({
        ...project,
        reviewedBy: 'U123',
        decisionReason: 'archived_or_fork',
        decisionNote: "Example sites aren't <tracked>.\nTry the main repo.",
      }),
    ).toBe(
      [
        `🚫 ${REPO_LABEL} wasn't added. <@U123> reviewed it: *Archived, fork or example repo*.`,
        "> Example sites aren't &lt;tracked&gt;.",
        '> Try the main repo.',
        'Reply here if this looks wrong.',
      ].join('\n'),
    )
    expect(
      buildBotRejectedReply({
        ...project,
        reviewedBy: 'U123',
        decisionReason: 'other',
        decisionNote: '   ',
      }),
    ).not.toContain('\n> ')
  })
})
