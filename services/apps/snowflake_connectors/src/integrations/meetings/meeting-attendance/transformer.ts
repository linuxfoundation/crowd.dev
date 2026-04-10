import { MEETINGS_GRID, MeetingsActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import {
  IActivityData,
  IOrganizationIdentity,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { TransformedActivity, TransformerBase } from '../../../core/transformerBase'

const log = getServiceChildLogger('meetingAttendanceTransformer')

function toISOTimestamp(rawDate: unknown, rawTime: unknown): string | null {
  const date =
    rawDate instanceof Date
      ? new Date(rawDate)
      : typeof rawDate === 'string'
        ? new Date(rawDate)
        : null
  if (!date || isNaN(date.getTime())) return null
  if (typeof rawTime === 'number') {
    date.setTime(date.getTime() + rawTime)
  } else if (typeof rawTime === 'string' && rawTime.trim()) {
    const combined = new Date(`${date.toISOString().slice(0, 10)}T${rawTime.trim()}Z`)
    if (!isNaN(combined.getTime())) return combined.toISOString()
  }
  return date.toISOString()
}

export class MeetingAttendanceTransformer extends TransformerBase {
  readonly platform = PlatformType.MEETINGS

  transformRow(row: Record<string, unknown>): TransformedActivity | TransformedActivity[] | null {
    const email = (row.INVITEE_EMAIL as string | null)?.trim() || null
    if (!email) {
      log.debug({ primaryKey: row.PRIMARY_KEY }, 'Skipping row: missing email')
      return null
    }

    const lfUsername = (row.INVITEE_LF_SSO as string | null)?.trim() || null
    const memberSourceId = (row.INVITEE_LF_USER_ID as string | null)?.trim() || undefined
    const generatedSourceId = (row.GENERATED_SOURCE_ID as string | null)?.trim() || undefined
    const userId = memberSourceId ?? generatedSourceId
    const displayName = (row.INVITEE_FULL_NAME as string | null)?.trim() || email

    const identities = this.buildMemberIdentities({
      email,
      sourceId: userId,
      platformUsername: null,
      lfUsername,
    })

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null
    if (!segmentSlug || !segmentSourceId) {
      return null
    }

    const timestamp = toISOTimestamp(row.MEETING_DATE, row.MEETING_TIME)

    const meetingId = (row.MEETING_ID as string)?.trim()

    const attributes = {
      meetingId,
      scheduledTime: timestamp,
      topic: (row.MEETING_NAME as string | null) || null,
      projectId: (row.PROJECT_ID as string | null) || null,
      projectName: (row.PROJECT_NAME as string | null) || null,
      organizationId: (row.ACCOUNT_ID as string | null) || null,
      organizationName: (row.ACCOUNT_NAME as string | null) || null,
      meetingType: (row.RAW_COMMITTEE_TYPE as string | null) || null,
    }

    const organizations = this.buildOrganizations(row)

    const buildActivity = (type: MeetingsActivityType): TransformedActivity => ({
      activity: {
        type,
        platform: PlatformType.MEETINGS,
        timestamp,
        score: MEETINGS_GRID[type].score,
        sourceId: `${meetingId}-${userId}`,
        member: {
          displayName,
          identities: [...identities],
          organizations: organizations ? [...organizations] : undefined,
        },
        attributes: { ...attributes },
      } as IActivityData,
      segment: { slug: segmentSlug, sourceId: segmentSourceId },
    })

    const activities: TransformedActivity[] = []

    if (row.WAS_INVITED === true) {
      activities.push(buildActivity(MeetingsActivityType.INVITED_MEETING))
    }

    if (row.INVITEE_ATTENDED === true) {
      activities.push(buildActivity(MeetingsActivityType.ATTENDED_MEETING))
    }

    if (activities.length === 0) {
      return null
    }

    return activities.length === 1 ? activities[0] : activities
  }

  private buildOrganizations(
    row: Record<string, unknown>,
  ): IActivityData['member']['organizations'] {
    const website = (row.ORG_WEBSITE as string | null)?.trim() || null
    const domainAliases = (row.ORG_DOMAIN_ALIASES as string | null)?.trim() || null

    if (!website && !domainAliases) {
      return undefined
    }

    const accountName = (row.ACCOUNT_NAME as string | null)?.trim() || null
    const displayName = accountName || website

    if (this.isIndividualNoAccount(displayName)) {
      return [
        {
          displayName,
          source: OrganizationSource.MEETINGS,
          identities: [
            {
              platform: this.platform,
              value: website,
              type: OrganizationIdentityType.PRIMARY_DOMAIN,
              verified: true,
            },
          ],
        },
      ]
    }

    const identities: IOrganizationIdentity[] = []

    if (website) {
      identities.push({
        platform: this.platform,
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
            platform: this.platform,
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
        source: OrganizationSource.MEETINGS,
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
