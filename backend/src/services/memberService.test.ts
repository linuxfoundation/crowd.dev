import { describe, expect, it } from 'vitest'

import { firstIdentityValue, normalizeDisplayName } from '@crowd/common'
import { MemberIdentityType } from '@crowd/types'

import { BasicMemberIdentity } from '@/database/repositories/types/memberTypes'

describe('MemberService.upsert displayName fallback', () => {
  it('falls back to the first identity value when displayName is missing', () => {
    const identities: BasicMemberIdentity[] = [
      { value: 'torvalds', type: MemberIdentityType.USERNAME },
    ]

    // this is the exact call memberService.ts makes at the displayName fallback site;
    // BasicMemberIdentity has no `.username` field, so a regression back to reading
    // `.username` here would make firstIdentityValue return undefined and this throw
    expect(normalizeDisplayName(firstIdentityValue(identities))).toBe('torvalds')
  })
})
