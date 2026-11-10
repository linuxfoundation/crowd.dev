import { describe, expect, it } from 'vitest'

import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

import {
  isRequestGroupMainRepository,
  mainRepositoriesFirst,
  sharedEvaluationOutcome,
  sharedOrganizationEvaluation,
} from './organizationEvaluation'

const source = 'https://lfx.slack.com/archives/C1/p1'

function row(overrides: Partial<IDbProjectCatalog>): IDbProjectCatalog {
  return {
    id: 'id',
    repoUrl: 'https://github.com/acme/widget',
    sourceUrl: source,
    requestHints: { organization: 'acme' },
    action: 'evaluate',
    classificationKind: 'non_lf_new_project',
    proposedAction: 'create_segment',
    pccProjectId: null,
    matchedSegmentId: null,
    evaluatedAt: null,
    evaluationResult: null,
    evaluationReason: null,
    reviewReason: null,
    ...overrides,
  } as IDbProjectCatalog
}

describe('isRequestGroupMainRepository', () => {
  it('recognises the repository named after its organization', () => {
    expect(isRequestGroupMainRepository(row({ repoUrl: 'https://github.com/acme/Acme' }))).toBe(
      true,
    )
    expect(isRequestGroupMainRepository(row({ repoUrl: 'https://github.com/acme/cli' }))).toBe(
      false,
    )
    expect(isRequestGroupMainRepository(row({ requestHints: null }))).toBe(false)
  })

  it('recognises the repository the request listed first in a project group', () => {
    const hints = {
      project: 'Apache HugeGraph',
      mainRepoUrl: 'https://github.com/apache/hugegraph',
    }
    expect(
      isRequestGroupMainRepository(
        row({ repoUrl: 'https://github.com/apache/hugegraph', requestHints: hints }),
      ),
    ).toBe(true)
    expect(
      isRequestGroupMainRepository(
        row({ repoUrl: 'https://github.com/apache/hugegraph-ai', requestHints: hints }),
      ),
    ).toBe(false)
    expect(
      isRequestGroupMainRepository(
        row({ repoUrl: 'https://github.com/apache/apache', requestHints: { project: 'Apache' } }),
      ),
    ).toBe(false)
  })
})

describe('mainRepositoriesFirst', () => {
  it('moves the main repository of each organization in front of its group and keeps the rest', () => {
    const rows = [
      row({ id: 'solo', repoUrl: 'https://github.com/solo/one', requestHints: null }),
      row({ id: 'a1', repoUrl: 'https://github.com/acme/tools' }),
      row({
        id: 'b1',
        repoUrl: 'https://github.com/beta/beta',
        requestHints: { organization: 'beta' },
      }),
      row({ id: 'a2', repoUrl: 'https://github.com/acme/acme' }),
      row({
        id: 'b2',
        repoUrl: 'https://github.com/beta/cli',
        requestHints: { organization: 'beta' },
      }),
      row({ id: 'a3', repoUrl: 'https://github.com/acme/cli' }),
    ]

    expect(mainRepositoriesFirst(rows).map((r) => r.id)).toEqual([
      'solo',
      'a2',
      'b1',
      'a1',
      'b2',
      'a3',
    ])
  })

  it('leaves an organization without a main repository in its order', () => {
    const rows = [row({ id: 'a1' }), row({ id: 'a2', repoUrl: 'https://github.com/acme/cli' })]

    expect(mainRepositoriesFirst(rows)).toEqual(rows)
  })
})

describe('sharedOrganizationEvaluation', () => {
  const lead = row({ id: 'lead' })

  it('takes the verdict of an evaluated sibling with the same proposal', () => {
    const siblings = [
      row({ id: 'pending' }),
      row({
        id: 'errored',
        action: 'error',
        evaluatedAt: '2026-10-19T10:00:00Z',
        evaluationResult: 'error',
        evaluationReason: 'API down',
      }),
      row({
        id: 'done',
        action: 'review',
        evaluatedAt: '2026-10-19T10:01:00Z',
        evaluationResult: 'true',
        evaluationReason: 'an active library',
        reviewReason: "Acme isn't in LFX Insights yet",
      }),
    ]

    expect(sharedOrganizationEvaluation(lead, siblings)).toEqual({
      action: 'review',
      evaluationResult: 'true',
      evaluationReason: 'an active library',
      reviewReason: "Acme isn't in LFX Insights yet",
    })
  })

  it('ignores siblings with another proposal or none evaluated yet', () => {
    expect(
      sharedOrganizationEvaluation(lead, [
        row({
          id: 'tracked',
          classificationKind: 'already_tracked',
          proposedAction: null,
          action: 'skip',
          evaluatedAt: '2026-10-19T10:00:00Z',
          evaluationResult: 'false',
        }),
        row({ id: 'pending' }),
      ]),
    ).toBeNull()
  })
})

describe('sharedEvaluationOutcome', () => {
  it('maps the stored verdict back to the evaluator outcome', () => {
    const base = { evaluationReason: null, reviewReason: null }
    expect(sharedEvaluationOutcome({ ...base, action: 'skip', evaluationResult: 'false' })).toBe(
      'skip',
    )
    expect(sharedEvaluationOutcome({ ...base, action: 'review', evaluationResult: 'true' })).toBe(
      'onboard',
    )
    expect(sharedEvaluationOutcome({ ...base, action: 'review', evaluationResult: 'unsure' })).toBe(
      'unsure',
    )
  })
})
