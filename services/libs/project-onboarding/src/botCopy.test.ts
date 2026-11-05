import { describe, expect, it } from 'vitest'

import {
  buildCdpAdminUrl,
  buildCdpSegmentUrl,
  describeReviewCase,
  repoLabel,
  technicalDetails,
} from './botCopy'

describe('botCopy', () => {
  it('labels a GitHub repository with its owner/repo path', () => {
    expect(repoLabel('https://github.com/acme/widget')).toBe(
      '*<https://github.com/acme/widget|acme/widget>*',
    )
    expect(repoLabel('https://gitlab.com/acme/<widget>')).toBe(
      '*https://gitlab.com/acme/&lt;widget&gt;*',
    )
  })

  it('describes every review case in plain words with the matched project name', () => {
    expect(describeReviewCase('add_to_connection', 'SLSA')).toEqual({
      insightsState: 'Project exists with GitHub connected',
      whatsNeeded: "Add this repo to SLSA's GitHub connection",
      action: 'add_to_connection',
    })
    expect(describeReviewCase('add_to_connection', 'SLSA', 12)).toMatchObject({
      whatsNeeded: "Add these 12 repos to SLSA's GitHub connection",
    })
    expect(describeReviewCase('connect_github', null)).toEqual({
      insightsState: 'Project exists, GitHub not connected',
      whatsNeeded: 'Connect GitHub to the project with this repo',
      action: 'connect_github',
    })
    expect(describeReviewCase('new_project_by_hand', 'Acme')).toEqual({
      insightsState: 'Not in LFX Insights yet',
      whatsNeeded:
        'Create Acme as a non-LF project in CDP with these repositories by hand, then mark as done',
      action: 'manual',
    })
    expect(describeReviewCase('not_in_pcc', 'Acme')).toEqual({
      insightsState: 'Not in the foundation project list',
      whatsNeeded: 'Check Acme with PCC, then create it in CDP, add this repo and mark as done',
      action: 'manual',
    })
    expect(describeReviewCase('legacy_connection', 'TiKV')).toEqual({
      insightsState: 'Uses the old GitHub connection',
      whatsNeeded: 'Add the repo by hand in CDP, then mark as done',
      action: 'manual',
    })
    expect(describeReviewCase('project_not_set_up', 'Rune DSL')).toEqual({
      insightsState: 'Not set up yet',
      whatsNeeded: 'Create Rune DSL in CDP, add this repo, then mark as done',
      action: 'manual',
    })
    expect(describeReviewCase('owner_only_match', null)).toEqual({
      insightsState: 'Only the GitHub owner matches LF projects',
      whatsNeeded: 'Confirm which project this repo belongs to, add it by hand, then mark as done',
      action: 'manual',
    })
    expect(describeReviewCase('unknown', null).action).toBe('manual')
    expect(describeReviewCase('handed_off', null, 12).whatsNeeded).toBe(
      'Confirm which project these 12 repos belong to, add them by hand, then mark as done',
    )
    expect(describeReviewCase('legacy_connection', 'TiKV', 3).whatsNeeded).toBe(
      'Add these 3 repos by hand in CDP, then mark as done',
    )
    expect(describeReviewCase('handed_off', null)).toEqual({
      insightsState: 'Handed to the CDP team by the requester',
      whatsNeeded: 'Confirm which project this repo belongs to, add it by hand, then mark as done',
      action: 'manual',
    })
    expect(describeReviewCase('new_project', 'Acme')).toEqual({
      insightsState: 'Not in LFX Insights yet',
      whatsNeeded: 'Create a new non-LF project in LFX Insights and add this repo',
      action: 'create_project',
    })
  })

  it('builds CDP links without doubling slashes', () => {
    expect(buildCdpSegmentUrl('https://cdp.test/', 'gp-1', 'seg-1')).toBe(
      'https://cdp.test/integrations/gp-1/seg-1',
    )
    expect(buildCdpAdminUrl('https://cdp.test')).toBe('https://cdp.test/admin#project-groups')
  })

  it('keeps the technical names in one details line', () => {
    expect(
      technicalDetails({ classificationKind: 'lf_in_cdp', proposedAction: 'update_integration' }),
    ).toBe('Details: lf_in_cdp · update_integration')
    expect(technicalDetails({ classificationKind: 'ambiguous', proposedAction: null })).toBe(
      'Details: ambiguous',
    )
    expect(technicalDetails({ classificationKind: null, proposedAction: null })).toBeNull()
  })
})
