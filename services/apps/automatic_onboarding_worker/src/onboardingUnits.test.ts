import { describe, expect, it } from 'vitest'

import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

import { groupOnboardingUnits } from './onboardingUnits'

const source = 'https://lfx.slack.com/archives/C1/p1'

function row(overrides: Partial<IDbProjectCatalog>): IDbProjectCatalog {
  return {
    id: 'id',
    repoUrl: 'https://github.com/acme/one',
    sourceUrl: source,
    requestHints: { organization: 'acme' },
    classificationKind: 'lf_in_cdp',
    proposedAction: 'update_integration',
    pccProjectId: 'pcc-1',
    matchedSegmentId: 'seg-1',
    ...overrides,
  } as IDbProjectCatalog
}

describe('groupOnboardingUnits', () => {
  it('connects the repositories of one organization to the same project in one unit', () => {
    const units = groupOnboardingUnits([
      row({ id: 'a' }),
      row({ id: 'b', repoUrl: 'https://github.com/acme/two' }),
      row({ id: 'c', repoUrl: 'https://github.com/acme/three', matchedSegmentId: 'seg-2' }),
      row({ id: 'd', repoUrl: 'https://github.com/other/one', requestHints: null }),
      row({ id: 'e', repoUrl: 'https://github.com/acme/four', proposedAction: 'create_segment' }),
    ])

    expect(
      units.map((unit) =>
        unit.kind === 'single' ? unit.project.id : unit.projects.map((p) => p.id),
      ),
    ).toEqual([['a', 'b'], 'c', 'd', ['e']])
  })

  it('creates one project for the repositories of a new organization', () => {
    const creation = {
      classificationKind: 'non_lf_new_project' as const,
      proposedAction: 'create_segment' as const,
      pccProjectId: null,
      matchedSegmentId: null,
    }
    const units = groupOnboardingUnits([
      row({ id: 'a', ...creation }),
      row({ id: 'b', repoUrl: 'https://github.com/acme/two', ...creation }),
      row({ id: 'c', repoUrl: 'https://github.com/acme/three' }),
      row({ id: 'd', repoUrl: 'https://github.com/acme/four', proposedAction: 'human_review' }),
    ])

    expect(
      units.map((unit) =>
        unit.kind === 'single' ? unit.project.id : unit.projects.map((p) => p.id),
      ),
    ).toEqual([['a', 'b'], 'c', 'd'])
  })

  it('keeps a lone organization row as a single onboarding unless it creates the project', () => {
    expect(groupOnboardingUnits([row({ id: 'a' })])).toEqual([
      { kind: 'single', project: row({ id: 'a' }) },
    ])
    const creation = row({
      id: 'a',
      classificationKind: 'non_lf_new_project',
      proposedAction: 'create_segment',
      pccProjectId: null,
      matchedSegmentId: null,
    })
    expect(groupOnboardingUnits([creation])).toEqual([
      { kind: 'organization', projects: [creation] },
    ])
  })
})
