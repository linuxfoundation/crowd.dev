import {
  IActivityData,
  IOrganizationIdentity,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { TransformerBase } from '../../core/transformerBase'

export abstract class TncTransformerBase extends TransformerBase {
  readonly platform = PlatformType.TNC

  protected buildOrganizations(
    row: Record<string, unknown>,
  ): IActivityData['member']['organizations'] {
    const website = (row.ORG_WEBSITE as string | null)?.trim() || null
    const domainAliases = (row.ORG_DOMAIN_ALIASES as string | null)?.trim() || null

    if (!website && !domainAliases) {
      return undefined
    }

    const identities: IOrganizationIdentity[] = []

    if (website) {
      identities.push({
        platform: PlatformType.TNC,
        value: website,
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      })
    }

    if (domainAliases) {
      for (const alias of domainAliases.split(',')) {
        const trimmed = alias.trim()
        if (trimmed) {
          identities.push({
            platform: PlatformType.TNC,
            value: trimmed,
            type: OrganizationIdentityType.ALTERNATIVE_DOMAIN,
            verified: true,
          })
        }
      }
    }

    return [
      {
        displayName: (row.ORGANIZATION_NAME as string | null)?.trim() || website,
        source: OrganizationSource.TNC,
        identities,
        logo: (row.LOGO_URL as string | null)?.trim() || undefined,
        size:
          typeof row.ORGANIZATION_SIZE === 'string'
            ? row.ORGANIZATION_SIZE.trim() || undefined
            : undefined,
        industry: (row.ORGANIZATION_INDUSTRY as string | null)?.trim() || undefined,
      },
    ]
  }
}
