import { describe, expect, it } from 'vitest'

import { firstIdentityValue } from '@crowd/common'
import { IOrganizationIdentity, OrganizationIdentityType } from '@crowd/types'

describe('OrganizationRepository.create displayName fallback', () => {
  it('falls back to the first identity value when displayName is missing', () => {
    const identities: IOrganizationIdentity[] = [
      {
        platform: 'github',
        value: 'torvalds',
        type: OrganizationIdentityType.USERNAME,
        verified: true,
      },
    ]

    // this is the exact call organizationRepository.ts makes at the displayName
    // fallback site; IOrganizationIdentity has no `.name` field, so a regression
    // back to reading `.name` here would make this return undefined
    expect(firstIdentityValue(identities)).toBe('torvalds')
  })
})
