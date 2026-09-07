import { describe, expect, it } from 'vitest'

import { IOrganizationIdentity, OrganizationIdentityType } from '@crowd/types'

function resolveOrganizationDisplayName(
  displayName: string | undefined,
  identities: IOrganizationIdentity[],
): string {
  if (!displayName) {
    return identities[0].value
  }
  return displayName
}

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

    expect(resolveOrganizationDisplayName(undefined, identities)).toBe('torvalds')
  })
})
