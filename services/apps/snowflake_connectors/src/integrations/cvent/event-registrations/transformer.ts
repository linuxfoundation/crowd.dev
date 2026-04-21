import { CVENT_GRID, CventActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import {
  IActivityData,
  IOrganizationIdentity,
  MemberAttributeName,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { TransformedActivity, TransformerBase } from '../../../core/transformerBase'

const log = getServiceChildLogger('cventTransformer')

export class CventTransformer extends TransformerBase {
  readonly platform = PlatformType.CVENT

  transformRow(row: Record<string, unknown>): TransformedActivity | null {
    const userName = (row.USER_NAME as string | null)?.trim() || null
    const lfUsername = (row.LFID as string | null)?.trim() || null
    const fullName = (row.FULL_NAME as string | null)?.trim() || null
    const firstName = (row.FIRST_NAME as string | null)?.trim() || null
    const lastName = (row.LAST_NAME as string | null)?.trim() || null
    const email = (row.EMAIL as string).trim()
    if (!email) {
      log.warn(
        { registrationId: row.REGISTRATION_ID, userName, lfUsername },
        'Skipping row: empty email',
      )
      return null
    }

    const registrationId = (row.REGISTRATION_ID as string)?.trim()
    const sourceId = (row.USER_ID as string | null) || undefined

    const displayName =
      fullName ||
      (firstName && lastName ? `${firstName} ${lastName}` : firstName || lastName) ||
      userName

    const identities = this.buildMemberIdentities({
      email,
      sourceId,
      platformUsername: userName,
      lfUsername,
    })

    const type =
      row.USER_ATTENDED === true
        ? CventActivityType.ATTENDED_EVENT
        : CventActivityType.REGISTERED_EVENT

    const timestamp = (row.REGISTRATION_CREATED_TS as string | null) || null

    const activity: IActivityData = {
      type,
      platform: PlatformType.CVENT,
      timestamp,
      score: CVENT_GRID[type].score,
      sourceId: registrationId,
      sourceParentId: row.EVENT_ID as string,
      member: {
        displayName,
        identities,
        organizations: this.buildOrganizations(row),
        attributes: {
          ...((row.JOB_TITLE as string | null) && {
            [MemberAttributeName.JOB_TITLE]: {
              [PlatformType.CVENT]: row.JOB_TITLE as string,
            },
          }),
          ...((row.USER_PHOTO_URL as string | null) && {
            [MemberAttributeName.AVATAR_URL]: {
              [PlatformType.CVENT]: row.USER_PHOTO_URL as string,
            },
          }),
          ...((row.USER_COUNTRY as string | null) && {
            [MemberAttributeName.COUNTRY]: {
              [PlatformType.CVENT]: row.USER_COUNTRY as string,
            },
          }),
        },
      },
      attributes: {
        eventName: row.EVENT_NAME as string,
        eventDate: (row.EVENT_START_DATE as string | null) || null,
        location: (row.EVENT_LOCATION as string | null) || null,
        website:
          (row.EVENT_URL as string | null) === 'NULL' ? null : (row.EVENT_URL as string) || null,
        registrationType: (row.REGISTRATION_TYPE as string | null) || null,
        attendeeType: (row.EVENT_ATTENDANCE_TYPE as string | null) || null,
        voucherCode: null,
        registrationRevenue:
          row.REGISTRATION_REVENUE != null ? String(row.REGISTRATION_REVENUE as number) : null,
      },
    }

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null
    if (!segmentSlug || !segmentSourceId) {
      return null
    }

    return { activity, segment: { slug: segmentSlug, sourceId: segmentSourceId } }
  }

  private buildOrganizations(
    row: Record<string, unknown>,
  ): IActivityData['member']['organizations'] {
    const website = (row.ORG_WEBSITE as string | null)?.trim() || null
    const domainAliases = (row.ORG_DOMAIN_ALIASES as string | null)?.trim() || null

    const hasDomain = website || domainAliases
    if (!hasDomain) {
      return undefined
    }

    const accountName = (row.ACCOUNT_NAME as string | null)?.trim() || null
    const displayName = accountName || website

    if (this.isIndividualNoAccount(displayName)) {
      return [
        {
          displayName,
          source: OrganizationSource.CVENT,
          identities: website
            ? [
                {
                  platform: PlatformType.CVENT,
                  value: website,
                  type: OrganizationIdentityType.PRIMARY_DOMAIN,
                  verified: true,
                },
              ]
            : [],
        },
      ]
    }

    const identities: IOrganizationIdentity[] = []

    if (website) {
      identities.push({
        platform: PlatformType.CVENT,
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
            platform: PlatformType.CVENT,
            value: trimmed,
            type: OrganizationIdentityType.ALTERNATIVE_DOMAIN,
            verified: true,
          })
        }
      }
    }

    return [
      {
        displayName,
        source: OrganizationSource.CVENT,
        identities,
        logo: (row.LOGO_URL as string | null)?.trim() || undefined,
        size: (row.ORGANIZATION_SIZE as string | null)?.trim() || undefined,
        industry: (row.ORGANIZATION_INDUSTRY as string | null)?.trim() || undefined,
      },
    ]
  }
}
