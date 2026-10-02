import { describe, expect, it } from 'vitest'

import { IRequestClassificationAlert } from './requestClassification'
import {
  buildRequestClassificationAlert,
  buildRequestClassificationAlertTitle,
} from './requestClassificationAlert'

const pccProject = { projectId: 'pcc-1', name: 'Acme', slug: 'acme', score: 0.98, isLeaf: true }

function alert(
  resolution: IRequestClassificationAlert['resolution'],
  dryRun = false,
): IRequestClassificationAlert {
  return {
    sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/1',
    repoUrls: ['https://github.com/acme/one'],
    resolution,
    dryRun,
  }
}

describe('buildRequestClassificationAlert', () => {
  it('links the discussion and lists the repositories', () => {
    const [intro] = buildRequestClassificationAlert(alert({ kind: 'lf_not_in_cdp', pccProject }))

    expect(intro.text).toContain('<https://github.com/linuxfoundation/insights/discussions/1|')
    expect(intro.text).toContain('https://github.com/acme/one')
  })

  it('shows the CDP segment, integration state and proposed action', () => {
    const sections = buildRequestClassificationAlert(
      alert({
        kind: 'lf_in_cdp',
        pccProject,
        segment: { segmentId: 'seg-1', name: 'Acme CDP', integration: 'github-nango' },
        action: 'update_integration',
      }),
    )

    const text = sections.map((section) => section.text).join('\n')
    expect(text).toContain('Acme CDP')
    expect(text).toContain('github-nango')
    expect(text).toContain('Update the existing GitHub connection')
  })

  it('shows the reason and the PCC candidates with their scores for an ambiguous request', () => {
    const sections = buildRequestClassificationAlert(
      alert({
        kind: 'ambiguous',
        reason: 'Project name only loosely matches PCC projects',
        candidates: [{ ...pccProject, score: 0.9 }],
      }),
    )

    const text = sections.map((section) => section.text).join('\n')
    expect(text).toContain('Project name only loosely matches PCC projects')
    expect(text).toContain('Acme (acme), score 0.90, project')
  })

  it('omits the candidates section when there are none', () => {
    const sections = buildRequestClassificationAlert(
      alert({ kind: 'ambiguous', reason: 'PCC lookup is not configured', candidates: [] }),
    )

    expect(sections.map((section) => section.title)).toEqual(['', 'Reason'])
  })
})

describe('buildRequestClassificationAlertTitle', () => {
  it('titles the alert after the resolution kind', () => {
    expect(
      buildRequestClassificationAlertTitle(alert({ kind: 'lf_not_in_pcc', projectName: 'Acme' })),
    ).toBe('LF onboarding request: project not found in PCC')
  })

  it('marks the title as a dry run', () => {
    expect(
      buildRequestClassificationAlertTitle(
        alert({ kind: 'lf_not_in_pcc', projectName: 'Acme' }, true),
      ),
    ).toBe('[DRY RUN] LF onboarding request: project not found in PCC')
  })
})

describe('dry run alert body', () => {
  it('states that nothing was written and nothing was onboarded', () => {
    const sections = buildRequestClassificationAlert(
      alert({ kind: 'non_lf_new_project', projectName: 'Acme' }, true),
    )

    expect(sections.map((section) => section.title)).toEqual(['', 'Outcome', 'Dry run'])
  })

  it('adds no dry run section for a live alert', () => {
    const sections = buildRequestClassificationAlert(
      alert({ kind: 'lf_not_in_pcc', projectName: 'Acme' }),
    )

    expect(sections.map((section) => section.title)).not.toContain('Dry run')
  })
})
