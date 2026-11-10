import { describe, expect, it } from 'vitest'

import {
  ingestProjectGroup,
  isReviewedDiscussionGroup,
  requestHintsByRepoUrl,
} from './requestHints'

describe('requestHintsByRepoUrl', () => {
  const one = 'https://github.com/acme/one'
  const two = 'https://github.com/acme/two'
  const orgRepo = 'https://github.com/openbmc/bmcweb'

  it('groups the listed repositories under the project name, the first one as main', () => {
    const hints = requestHintsByRepoUrl([one, two], [], { projectName: ' Acme ' })

    expect([...hints.entries()]).toEqual([
      [one, { project: 'Acme', mainRepoUrl: one }],
      [two, { project: 'Acme', mainRepoUrl: one }],
    ])
    expect(ingestProjectGroup(hints, [one, two])).toEqual({ name: 'Acme', repositories: 2 })
  })

  it('leaves a single repository, or repositories without a name, ungrouped', () => {
    expect(requestHintsByRepoUrl([one], [], { projectName: 'Acme' }).size).toBe(0)
    expect(requestHintsByRepoUrl([one, two], [], { projectName: null }).size).toBe(0)
    expect(ingestProjectGroup(new Map(), [one])).toBeNull()
  })

  it('carries the suggested slug and the parser confidence into the group hints', () => {
    const hints = requestHintsByRepoUrl([one, two], [], {
      projectName: 'Acme',
      suggestedSlug: 'acme-tools',
      confidence: 0.85,
    })

    expect(hints.get(two)).toEqual({
      project: 'Acme',
      mainRepoUrl: one,
      suggestedSlug: 'acme-tools',
      confidence: 0.85,
    })
    expect(
      requestHintsByRepoUrl([one, two], [], { projectName: 'Acme', confidence: null }).get(one),
    ).toEqual({
      project: 'Acme',
      mainRepoUrl: one,
    })
  })

  it('carries the LF declaration into the group hints when the request states it', () => {
    expect(
      requestHintsByRepoUrl([one, two], [], { projectName: 'Acme', declaredLf: true }).get(one),
    ).toEqual({ project: 'Acme', mainRepoUrl: one, declaredLf: true })
    expect(
      requestHintsByRepoUrl([one, two], [], { projectName: 'Acme', declaredLf: null }).get(one),
    ).toEqual({ project: 'Acme', mainRepoUrl: one })
  })

  it('counts only the repositories that ended up in this request', () => {
    const hints = requestHintsByRepoUrl([one, two], [], { projectName: 'Acme' })

    expect(ingestProjectGroup(hints, [one])).toBeNull()
    expect(ingestProjectGroup(hints, [two, one])).toEqual({ name: 'Acme', repositories: 2 })
  })

  it('keeps the owner hint for expanded organizations and groups the rest by name', () => {
    const hints = requestHintsByRepoUrl(
      [orgRepo, one, two],
      [{ owner: 'openbmc', repoUrls: [orgRepo] }],
      { projectName: 'Acme' },
    )

    expect(hints.get(orgRepo)).toEqual({ organization: 'openbmc' })
    expect(hints.get(one)).toEqual({ project: 'Acme', mainRepoUrl: one })
    expect(ingestProjectGroup(hints, [orgRepo, one, two])).toEqual({
      name: 'Acme',
      repositories: 2,
    })
  })
})

describe('isReviewedDiscussionGroup', () => {
  const hints = { project: 'Acme', mainRepoUrl: 'https://github.com/acme/one' }

  it('is true only for a discussion row that belongs to a group', () => {
    expect(
      isReviewedDiscussionGroup({ provenance: 'github-discussion', requestHints: hints }),
    ).toBe(true)
    expect(
      isReviewedDiscussionGroup({
        provenance: 'github-discussion',
        requestHints: { organization: 'acme' },
      }),
    ).toBe(true)
    expect(isReviewedDiscussionGroup({ provenance: 'github-discussion', requestHints: null })).toBe(
      false,
    )
    expect(isReviewedDiscussionGroup({ provenance: 'github-discussion' })).toBe(false)
    expect(isReviewedDiscussionGroup({ provenance: 'slack-bot', requestHints: hints })).toBe(false)
  })
})
