import { describe, expect, it } from 'vitest'

import { organizationFailureNote } from './discussionNotes'
import { IOrganizationExpansionFailure } from './organizationRequest'

function failure(
  kind: IOrganizationExpansionFailure['kind'],
  owner = 'openbmc',
): IOrganizationExpansionFailure {
  return { ok: false, kind, owner, limit: 50, reply: 'slack', reason: 'reason' }
}

describe('organizationFailureNote', () => {
  it('tells the requester in markdown what to change in the discussion', () => {
    expect(organizationFailureNote(failure('not_found', 'no_body'))).toBe(
      "I couldn't find a GitHub organization called **github.com/no\\_body**. Check the link, or edit this discussion with the repository links themselves.",
    )
    expect(organizationFailureNote(failure('unavailable'))).toContain(
      "couldn't list the repositories of **github.com/openbmc**. I'll try again at the next run.",
    )
    expect(organizationFailureNote(failure('too_large'))).toBe(
      '**github.com/openbmc** has more than 50 public repositories, and for now I take up to 50 at a time. Edit this discussion with the repositories to add first, or ask the CDP team to connect the whole organization from CDP.',
    )
    expect(organizationFailureNote(failure('too_large_together'))).toContain(
      'Together, these organizations have more than 50 public repositories',
    )
    expect(organizationFailureNote(failure('empty'))).toBe(
      '**github.com/openbmc** has no public repositories I can add: archived repositories and forks are left out.',
    )
  })
})
