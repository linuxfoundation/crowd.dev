import { MEETINGS_GRID, MeetingsActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import { IActivityData, OrganizationSource, PlatformType } from '@crowd/types'

import { TransformedActivity, TransformerBase } from '../../../core/transformerBase'

const log = getServiceChildLogger('meetingAttendanceTransformer')

export class MeetingAttendanceTransformer extends TransformerBase {
  readonly platform = PlatformType.MEETINGS

  transformRow(row: Record<string, unknown>): TransformedActivity | TransformedActivity[] | null {
    const email = (row.INVITEE_EMAIL as string | null)?.trim() || null
    if (!email) {
      log.debug({ primaryKey: row.PRIMARY_KEY }, 'Skipping row: missing email')
      return null
    }

    const lfUsername = (row.INVITEE_LF_SSO as string | null)?.trim() || null
    const sourceId = (row.INVITEE_LF_USER_ID as string | null)?.trim() || undefined
    const displayName = (row.INVITEE_FULL_NAME as string | null)?.trim() || email

    const identities = this.buildMemberIdentities({
      email,
      sourceId,
      platformUsername: null,
      lfUsername,
    })

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null
    if (!segmentSlug || !segmentSourceId) {
      return null
    }

    const meetingDate = (row.MEETING_DATE as string | null) || null
    const meetingTime = (row.MEETING_TIME as string | null) || null
    const timestamp =
      meetingDate && meetingTime ? `${meetingDate}T${meetingTime}` : meetingDate || null

    const primaryKey = (row.PRIMARY_KEY as string)?.trim()

    const attributes = {
      meetingID: row.MEETING_ID,
      scheduledTime: timestamp,
      topic: (row.MEETING_NAME as string | null) || null,
      projectID: (row.PROJECT_ID as string | null) || null,
      projectName: (row.PROJECT_NAME as string | null) || null,
      organizationId: (row.ACCOUNT_ID as string | null) || null,
      organizationName: (row.ACCOUNT_NAME as string | null) || null,
      meetingType: (row.RAW_COMMITTEE_TYPE as string | null) || null,
    }

    const member = {
      displayName,
      identities,
      organizations: this.buildOrganizations(row),
    }

    const segment = { slug: segmentSlug, sourceId: segmentSourceId }

    const activities: TransformedActivity[] = []

    if (row.WAS_INVITED === true) {
      activities.push({
        activity: {
          type: MeetingsActivityType.INVITED_MEETING,
          platform: PlatformType.MEETINGS,
          timestamp,
          score: MEETINGS_GRID[MeetingsActivityType.INVITED_MEETING].score,
          sourceId: `${primaryKey}_invited`,
          member: { ...member },
          attributes,
        } as IActivityData,
        segment,
      })
    }

    if (row.INVITEE_ATTENDED === true) {
      activities.push({
        activity: {
          type: MeetingsActivityType.ATTENDED_MEETING,
          platform: PlatformType.MEETINGS,
          timestamp,
          score: MEETINGS_GRID[MeetingsActivityType.ATTENDED_MEETING].score,
          sourceId: `${primaryKey}_attended`,
          member: { ...member },
          attributes,
        } as IActivityData,
        segment,
      })
    }

    if (activities.length === 0) {
      return null
    }

    return activities.length === 1 ? activities[0] : activities
  }

  private buildOrganizations(
    row: Record<string, unknown>,
  ): IActivityData['member']['organizations'] {
    const accountName = (row.ACCOUNT_NAME as string | null)?.trim() || null
    if (!accountName) {
      return undefined
    }

    if (this.isIndividualNoAccount(accountName)) {
      return undefined
    }

    return [
      {
        displayName: accountName,
        source: OrganizationSource.MEETINGS,
        identities: [],
      },
    ]
  }
}
