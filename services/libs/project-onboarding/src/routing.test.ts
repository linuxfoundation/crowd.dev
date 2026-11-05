import { describe, expect, it } from 'vitest'

import { ICdpSegmentMatch, IPccCandidate, OnboardingResolution } from './requestResolver'
import { routeResolution, routeEvaluationOutcome } from './routing'

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

function lfInCdp(action: 'create_integration' | 'update_integration' | 'human_review') {
  return { kind: 'lf_in_cdp', pccProject, segment, action } as const
}

describe('routeResolution', () => {
  it.each<[OnboardingResolution, string | null]>([
    [{ kind: 'non_lf_new_project', projectName: 'Acme' }, null],
    [{ kind: 'non_lf_new_project', projectName: null }, null],
  ])('routes %j to the LLM evaluation', (resolution) => {
    expect(routeResolution(resolution, { autoApply: false })).toMatchObject({
      action: 'evaluate',
      skipReason: null,
      reviewReason: null,
    })
    expect(routeResolution(resolution, { autoApply: true })).toMatchObject({
      action: 'evaluate',
      skipReason: null,
      reviewReason: null,
    })
  })

  it.each(['create_integration', 'update_integration'] as const)(
    'routes an LF project in CDP with %s to review unless auto apply is on',
    (action) => {
      expect(routeResolution(lfInCdp(action), { autoApply: false })).toMatchObject({
        action: 'review',
        skipReason: null,
        reviewReason:
          'Acme is in LFX Insights, but this repo needs a quick check by the CDP team before I add it',
      })
      expect(routeResolution(lfInCdp(action), { autoApply: true })).toMatchObject({
        action: 'onboard',
        skipReason: null,
        reviewReason: null,
      })
    },
  )

  it.each<[OnboardingResolution, string]>([
    [
      lfInCdp('human_review'),
      "Acme still uses an older GitHub connection, so I can't add repositories to it myself",
    ],
    [
      { kind: 'lf_not_in_cdp', pccProject },
      "Acme is an LF project, but it isn't set up in LFX Insights yet",
    ],
    [
      { kind: 'lf_owner_in_cdp', owners: ['acme', 'acme-labs'] },
      "the GitHub owners acme, acme-labs only have LF projects in LFX Insights, so I couldn't tell which one this repo belongs to",
    ],
    [
      { kind: 'lf_not_in_pcc', projectName: 'Acme' },
      "Acme is described as an LF project, but it isn't in the foundation project list yet",
    ],
  ])('routes %j to review regardless of auto apply', (resolution, reviewReason) => {
    expect(routeResolution(resolution, { autoApply: false })).toMatchObject({
      action: 'review',
      skipReason: null,
      reviewReason,
    })
    expect(routeResolution(resolution, { autoApply: true })).toMatchObject({
      action: 'review',
      skipReason: null,
      reviewReason,
    })
  })

  it.each<[OnboardingResolution, string]>([
    [
      { kind: 'already_tracked', repoUrls: ['https://github.com/acme/one'] },
      'it is already in LFX Insights',
    ],
    [
      { kind: 'non_github_source', nonGithubRepoUrls: ['https://gitlab.com/acme/one'] },
      'only GitHub repositories can be added',
    ],
    [
      { kind: 'ambiguous', reason: "that's a question I can't decide", candidates: [] },
      "that's a question I can't decide",
    ],
  ])('skips %j regardless of auto apply', (resolution, skipReason) => {
    expect(routeResolution(resolution, { autoApply: false })).toMatchObject({
      action: 'skip',
      skipReason,
      reviewReason: null,
    })
    expect(routeResolution(resolution, { autoApply: true })).toMatchObject({
      action: 'skip',
      skipReason,
      reviewReason: null,
    })
  })
})

describe('routeEvaluationOutcome', () => {
  const botRow = { provenance: 'slack-bot' as const, projectName: 'Acme', repoName: 'widget' }
  const verdict = {
    outcome: 'onboard' as const,
    evaluationResult: 'onboard',
    evaluationReason: null,
  }

  it('sends a new project from the bot to review while auto apply is off', () => {
    expect(routeEvaluationOutcome(botRow, verdict, { autoApply: false })).toEqual({
      action: 'review',
      reviewReason:
        "Acme isn't in LFX Insights yet; it would be created as a non-LF project, and new projects are added by the CDP team",
    })
    expect(
      routeEvaluationOutcome({ ...botRow, projectName: null }, verdict, { autoApply: false })
        .reviewReason,
    ).toContain("widget isn't in LFX Insights yet")
  })

  it('names the project an organization would become and counts its repositories', () => {
    expect(
      routeEvaluationOutcome(botRow, verdict, {
        autoApply: false,
        organization: { owner: 'acme', repositories: 12 },
      }).reviewReason,
    ).toBe(
      "Acme isn't in LFX Insights yet; it would be created as a non-LF project with the 12 repositories of github.com/acme, and new projects are added by the CDP team",
    )
    expect(
      routeEvaluationOutcome({ ...botRow, projectName: null }, verdict, {
        autoApply: false,
        organization: { owner: 'ai-dynamo', repositories: 1 },
      }).reviewReason,
    ).toContain(
      "Ai Dynamo isn't in LFX Insights yet; it would be created as a non-LF project with the 1 repository of github.com/ai-dynamo",
    )
  })

  it('lets the row onboard when auto apply is on or the request did not come from the bot', () => {
    expect(routeEvaluationOutcome(botRow, verdict, { autoApply: true })).toEqual({
      action: 'onboard',
      reviewReason: null,
    })
    expect(
      routeEvaluationOutcome({ ...botRow, provenance: 'github-discussion' }, verdict, {
        autoApply: false,
      }),
    ).toEqual({ action: 'onboard', reviewReason: null })
  })

  it('sends an undecided verdict to review with the evaluator reason', () => {
    expect(
      routeEvaluationOutcome(
        botRow,
        { outcome: 'unsure', evaluationResult: 'unsure', evaluationReason: ' mixed signals ' },
        { autoApply: false },
      ),
    ).toEqual({
      action: 'review',
      reviewReason: "the automatic check couldn't decide: mixed signals",
    })
    expect(
      routeEvaluationOutcome(
        botRow,
        { outcome: 'unsure', evaluationResult: 'unsure', evaluationReason: null },
        { autoApply: true },
      ).reviewReason,
    ).toBe("the automatic check couldn't decide")
  })

  it('turns evaluator errors into error rows and keeps skips as they are', () => {
    expect(
      routeEvaluationOutcome(
        botRow,
        { outcome: 'unsure', evaluationResult: 'error', evaluationReason: 'API returned HTTP 500' },
        { autoApply: false },
      ),
    ).toEqual({ action: 'error', reviewReason: null })
    expect(
      routeEvaluationOutcome(
        botRow,
        { outcome: 'skip', evaluationResult: 'skip', evaluationReason: 'a fork' },
        { autoApply: false },
      ),
    ).toEqual({ action: 'skip', reviewReason: null })
  })
})

describe('routeResolution for questions', () => {
  const question = {
    kind: 'did_you_mean' as const,
    text: 'Did you mean Acme Labs?',
    options: [{ id: 'yes', label: 'Yes, Acme Labs' }],
  }
  const resolution: OnboardingResolution = {
    kind: 'needs_input',
    question,
    fallbackReason: 'the name only loosely matches projects in the foundation project list',
    candidates: [],
  }

  it('asks the requester when the request came from someone who can answer', () => {
    expect(routeResolution(resolution, { autoApply: false, canAsk: true })).toEqual({
      action: 'ask',
      skipReason: null,
      reviewReason: null,
      requestQuestion: question,
    })
  })

  it('falls back to review when nobody can be asked', () => {
    expect(routeResolution(resolution, { autoApply: false })).toEqual({
      action: 'review',
      skipReason: null,
      reviewReason: 'the name only loosely matches projects in the foundation project list',
      requestQuestion: null,
    })
  })
})
