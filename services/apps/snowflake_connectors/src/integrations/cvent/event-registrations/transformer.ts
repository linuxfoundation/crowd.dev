import { CVENT_GRID, CventActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import {
  IActivityData,
  IMemberData,
  IOrganizationIdentity,
  MemberAttributeName,
  MemberIdentityType,
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
    const email = (row.EMAIL as string | null)?.trim() || null
    if (!email) {
      log.debug(
        { registrationId: row.REGISTRATION_ID, userName, lfUsername },
        'Skipping row: missing email',
      )
      return null
    }

    const registrationId = (row.REGISTRATION_ID as string)?.trim()

    const displayName =
      fullName ||
      (firstName && lastName ? `${firstName} ${lastName}` : firstName || lastName) ||
      userName

    const identities: IMemberData['identities'] = []
    const sourceId = (row.USER_ID as string | null) || undefined

    if (userName) {
      identities.push(
        {
          platform: PlatformType.CVENT,
          value: email,
          type: MemberIdentityType.EMAIL,
          verified: true,
          verifiedBy: PlatformType.CVENT,
          sourceId,
        },
        {
          platform: PlatformType.CVENT,
          value: userName,
          type: MemberIdentityType.USERNAME,
          verified: true,
          verifiedBy: PlatformType.CVENT,
          sourceId,
        },
      )
    } else {
      identities.push({
        platform: PlatformType.CVENT,
        value: email,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.CVENT,
        sourceId,
      })
    }

    if (lfUsername) {
      identities.push({
        platform: PlatformType.LFID,
        value: lfUsername,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.CVENT,
        sourceId,
      })
    }

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

    const identities: IOrganizationIdentity[] = []

    const accountName = (row.ACCOUNT_NAME as string | null)?.trim() || null

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
        displayName: accountName || website,
        source: OrganizationSource.CVENT,
        identities,
        logo: (row.LOGO_URL as string | null)?.trim() || undefined,
        size: (row.ORGANIZATION_SIZE as string | null)?.trim() || undefined,
        industry: (row.ORGANIZATION_INDUSTRY as string | null)?.trim() || undefined,
      },
    ]
  }
}
