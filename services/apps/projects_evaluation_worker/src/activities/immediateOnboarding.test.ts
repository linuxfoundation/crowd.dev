import { describe, expect, it } from 'vitest'

import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

import { buildImmediateOnboardingPlan } from './immediateOnboarding'

function row(overrides: Partial<IDbProjectCatalog>): IDbProjectCatalog {
  return {
    id: 'id-1',
    projectSlug: 'acme-widget',
    repoName: 'widget',
    repoUrl: 'https://github.com/acme/widget',
    source: 'slack-bot',
    sourceUrl: null,
    provenance: 'slack-bot',
    action: 'onboard',
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

describe('buildImmediateOnboardingPlan', () => {
  const bot = row({ id: 'bot' })
  const discovery = row({
    id: 'discovery',
    source: 'insights-discussions',
    provenance: 'github-discussion',
  })

  const botRef = { id: 'bot', repoUrl: 'https://github.com/acme/widget', sourceUrl: null }

  it('onboards slack bot rows right away when auto apply is on', () => {
    expect(buildImmediateOnboardingPlan([bot, discovery], true)).toEqual({ toOnboard: [botRef] })
  })

  it('leaves every row to the scheduled run when auto apply is off', () => {
    expect(buildImmediateOnboardingPlan([bot, discovery], false)).toEqual({ toOnboard: [] })
  })

  it('never onboards rows that did not come from the slack bot', () => {
    expect(buildImmediateOnboardingPlan([discovery], true)).toEqual({ toOnboard: [] })
  })
})
