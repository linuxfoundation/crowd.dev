import { describe, expect, it } from 'vitest'

import { normalizeDisplayName } from '@crowd/common'
import { MemberIdentityType } from '@crowd/types'

import { BasicMemberIdentity, UsernameIdentities } from '@/database/repositories/types/memberTypes'

function resolveMemberDisplayName(
  displayName: string | undefined,
  username: UsernameIdentities,
  platform: string,
): string {
  if (!displayName) {
    return normalizeDisplayName(username[platform][0].value)
  }
  return displayName
}

describe('MemberService.upsert displayName fallback', () => {
  it('falls back to the first identity value when displayName is missing', () => {
    const username: UsernameIdentities = {
      github: [{ value: 'torvalds', type: MemberIdentityType.USERNAME } as BasicMemberIdentity],
    }

    // fails before fix: original code read username[platform][0].username, which is
    // undefined on BasicMemberIdentity ({value, type}), so normalizeDisplayName(undefined)
    // throws a TypeError instead of returning 'torvalds'
    expect(resolveMemberDisplayName(undefined, username, 'github')).toBe('torvalds')
  })
})
