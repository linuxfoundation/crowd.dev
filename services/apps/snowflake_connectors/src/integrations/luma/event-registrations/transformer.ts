import { LUMA_GRID, LumaActivityType } from '@crowd/integrations'
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

import { cleanPersonalEmail } from '../../../core/personalEmail'
import {
  TransformedActivity,
  TransformerBase,
  cleanIdentityValue,
} from '../../../core/transformerBase'

const log = getServiceChildLogger('lumaEventRegistrationsTransformer')

const ACCEPTED_STATUS = 'accepted'

const TIMEZONE_SUFFIX_RE = /(Z|[+-]\d{2}:?\d{2})$/i

function cleanText(raw: unknown): string | null {
  return typeof raw === 'string' ? raw.trim() || null : null
}

function toIsoTimestamp(raw: unknown): string | null {
  const date =
    raw instanceof Date
      ? raw
      : typeof raw === 'string' && raw.trim()
        ? new Date(TIMEZONE_SUFFIX_RE.test(raw.trim()) ? raw.trim() : `${raw.trim()}Z`)
        : null
  return date && !isNaN(date.getTime()) ? date.toISOString() : null
}

function toIsoDate(raw: unknown): string | null {
  return toIsoTimestamp(raw)?.slice(0, 10) ?? null
}

function joinPresent(parts: (string | null)[]): string | null {
  return parts.filter(Boolean).join(', ') || null
}

export class LumaEventRegistrationsTransformer extends TransformerBase {
  readonly platform = PlatformType.LUMA

  transformRow(row: Record<string, unknown>): TransformedActivity[] | null {
    const registrationId = cleanText(row.REGISTRATION_ID)
    const email = cleanPersonalEmail(row.EMAIL)
    if (!registrationId || !email) {
      log.warn({ registrationId }, 'Skipping row: missing registration id or email')
      return null
    }

    const segmentSlug = cleanText(row.PROJECT_SLUG)
    const segmentSourceId = cleanText(row.PROJECT_ID)
    if (!segmentSlug || !segmentSourceId) {
      log.warn({ registrationId }, 'Skipping row: missing project')
      return null
    }

    const lfUsername = cleanIdentityValue(row.LFID)
    const member = this.buildMember(row, email, lfUsername)
    const activityFields = this.buildActivityFields(row, registrationId)

    const buildActivity = (type: LumaActivityType, timestamp: string): TransformedActivity => ({
      activity: {
        ...activityFields,
        type,
        timestamp,
        score: LUMA_GRID[type].score,
        member: {
          ...member,
          identities: [...member.identities],
          organizations: member.organizations ? [...member.organizations] : undefined,
          attributes: { ...member.attributes },
        },
        attributes: { ...activityFields.attributes },
      },
      segment: { slug: segmentSlug, sourceId: segmentSourceId },
    })

    const activities: TransformedActivity[] = []

    const registeredAt = toIsoTimestamp(row.REGISTRATION_TS)
    if (this.isAccepted(row) && registeredAt) {
      activities.push(buildActivity(LumaActivityType.REGISTERED_EVENT, registeredAt))
    }

    const checkedInAt = toIsoTimestamp(row.USER_CHECKED_IN_TS)
    if (row.USER_ATTENDED === true && checkedInAt) {
      activities.push(buildActivity(LumaActivityType.ATTENDED_EVENT, checkedInAt))
    }

    if (activities.length === 0) {
      log.warn({ registrationId }, 'Skipping row: neither an accepted registration nor a check-in')
      return null
    }

    return activities
  }

  private isAccepted(row: Record<string, unknown>): boolean {
    return cleanText(row.REGISTRATION_STATUS)?.toLowerCase() === ACCEPTED_STATUS
  }

  private buildActivityFields(
    row: Record<string, unknown>,
    registrationId: string,
  ): Pick<IActivityData, 'platform' | 'sourceId' | 'sourceParentId' | 'url' | 'attributes'> {
    const eventUrl = cleanText(row.EVENT_URL)
    return {
      platform: PlatformType.LUMA,
      sourceId: registrationId,
      sourceParentId: cleanText(row.EVENT_ID) ?? undefined,
      url: eventUrl ?? undefined,
      attributes: {
        eventName: cleanText(row.EVENT_NAME),
        eventDate: toIsoDate(row.EVENT_START_DATE),
        location: joinPresent([cleanText(row.EVENT_CITY), cleanText(row.EVENT_COUNTRY)]),
        website: eventUrl,
        registrationType: cleanText(row.REGISTRATION_TYPE),
        eventType: cleanText(row.EVENT_REGISTRATION_TYPE),
      },
    }
  }

  private buildMember(
    row: Record<string, unknown>,
    email: string,
    lfUsername: string | null,
  ): IMemberData {
    const fullName = cleanText(row.FULL_NAME)
    const nameFromParts = [cleanText(row.FIRST_NAME), cleanText(row.LAST_NAME)]
      .filter(Boolean)
      .join(' ')
    const jobTitle = cleanText(row.JOB_TITLE)
    const avatarUrl = cleanText(row.USER_PHOTO_URL)
    const country = cleanText(row.USER_COUNTRY)

    return {
      displayName: fullName || nameFromParts || lfUsername || email,
      identities: this.buildIdentities(email, lfUsername),
      organizations: this.buildOrganizations(row),
      attributes: {
        ...(jobTitle && {
          [MemberAttributeName.JOB_TITLE]: { [PlatformType.LUMA]: jobTitle },
        }),
        ...(avatarUrl && {
          [MemberAttributeName.AVATAR_URL]: { [PlatformType.LUMA]: avatarUrl },
        }),
        ...(country && {
          [MemberAttributeName.COUNTRY]: { [PlatformType.LUMA]: country },
        }),
      },
    }
  }

  private buildIdentities(email: string, lfUsername: string | null): IMemberData['identities'] {
    const unverifiedEmail = {
      platform: PlatformType.LUMA,
      value: email,
      type: MemberIdentityType.EMAIL,
      verified: false,
    }

    if (!lfUsername) {
      return [
        {
          platform: PlatformType.LUMA,
          value: email,
          type: MemberIdentityType.USERNAME,
          verified: true,
          verifiedBy: PlatformType.LUMA,
        },
        unverifiedEmail,
      ]
    }

    return [
      {
        platform: PlatformType.LFID,
        value: lfUsername,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.LUMA,
      },
      {
        platform: PlatformType.LUMA,
        value: lfUsername,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.LUMA,
      },
      unverifiedEmail,
    ]
  }

  private buildOrganizations(row: Record<string, unknown>): IMemberData['organizations'] {
    const website = cleanText(row.ORG_WEBSITE)
    const domainAliases = cleanText(row.ORG_DOMAIN_ALIASES)
    if (!website && !domainAliases) {
      return undefined
    }

    const displayName = cleanText(row.ACCOUNT_NAME) || website

    const identities: IOrganizationIdentity[] = []
    if (website) {
      identities.push({
        platform: PlatformType.LUMA,
        value: website,
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      })
    }

    if (this.isIndividualNoAccount(displayName)) {
      return [{ displayName, source: OrganizationSource.LUMA, identities }]
    }

    for (const alias of (domainAliases ?? '').split(',')) {
      const trimmed = alias.trim()
      if (trimmed) {
        identities.push({
          platform: PlatformType.LUMA,
          value: trimmed,
          type: OrganizationIdentityType.ALTERNATIVE_DOMAIN,
          verified: true,
        })
      }
    }

    return [
      {
        displayName,
        source: OrganizationSource.LUMA,
        identities,
        logo: cleanText(row.LOGO_URL) ?? undefined,
        size: cleanText(row.ORGANIZATION_SIZE) ?? undefined,
        industry: cleanText(row.ORGANIZATION_INDUSTRY) ?? undefined,
      },
    ]
  }
}
