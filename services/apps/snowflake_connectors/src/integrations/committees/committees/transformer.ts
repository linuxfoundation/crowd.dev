import { COMMITTEES_GRID, CommitteesActivityType } from '@crowd/integrations'
import { getServiceChildLogger } from '@crowd/logging'
import {
  IActivityData,
  IOrganizationIdentity,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { TransformedActivity, TransformerBase } from '../../../core/transformerBase'

const log = getServiceChildLogger('committeesCommitteesTransformer')

export class CommitteesCommitteesTransformer extends TransformerBase {
  readonly platform = PlatformType.COMMITTEES

  transformRow(row: Record<string, unknown>): TransformedActivity | null {
    const email = (row.CONTACTEMAIL__C as string | null)?.trim() || null
    if (!email) {
      log.warn(
        { sfid: row.SFID, committeeId: row.COMMITTEE_ID, rawEmail: row.CONTACTEMAIL__C },
        'Skipping row: missing email',
      )
      return null
    }

    const committeeId = (row.COMMITTEE_ID as string).trim()
    const fivetranDeleted = row.FIVETRAN_DELETED as boolean
    const lfUsername = (row.LF_USERNAME as string | null)?.trim() || null
    const suFullName = (row.SU_FULL_NAME as string | null)?.trim() || null
    const suFirstName = (row.SU_FIRST_NAME as string | null)?.trim() || null
    const suLastName = (row.SU_LAST_NAME as string | null)?.trim() || null

    const displayName =
      suFullName ||
      (suFirstName && suLastName ? `${suFirstName} ${suLastName}` : suFirstName || suLastName) ||
      email.split('@')[0]

    const type = fivetranDeleted
      ? CommitteesActivityType.REMOVED_FROM_COMMITTEE
      : CommitteesActivityType.ADDED_TO_COMMITTEE

    const sourceId = (row.PRIMARY_SOURCE_USER_ID as string | null)?.trim() || undefined
    const identities = this.buildMemberIdentities({
      email,
      platformUsername: null,
      sourceId,
      lfUsername,
    })

    const activityTimestamp =
      type === CommitteesActivityType.ADDED_TO_COMMITTEE
        ? (row.CREATEDDATE as string | null) || null
        : (row.FIVETRAN_SYNCED as string | null) || null

    const committeeName = (row.COMMITTEE_NAME as string | null) || null

    const activity: IActivityData = {
      type,
      platform: PlatformType.COMMITTEES,
      timestamp: activityTimestamp,
      score: COMMITTEES_GRID[type].score,
      sourceId: `${committeeId}-${row.SFID}`,
      sourceParentId: null,
      channel: committeeName,
      member: {
        displayName,
        identities,
        organizations: this.buildOrganizations(row),
      },
      attributes: {
        committeeId: (row.COLLABORATION_NAME__C as string | null) || null,
        committeeName,
        role: (row.ROLE__C as string | null) || null,
        projectId: (row.PROJECT_ID as string | null) || null,
        projectName: (row.PROJECT_NAME as string | null) || null,
        organizationId: (row.ACCOUNT__C as string | null) || null,
        organizationName: (row.ACCOUNT_NAME as string | null) || null,
      },
    }

    const segmentSlug = (row.PROJECT_SLUG as string | null)?.trim() || null
    const segmentSourceId = (row.PROJECT_ID as string | null)?.trim() || null

    if (!segmentSlug || !segmentSourceId) {
      log.warn(
        { sfid: row.SFID, committeeId, segmentSlug, segmentSourceId },
        'Skipping row: missing segment slug or sourceId',
      )
      return null
    }

    return { activity, segment: { slug: segmentSlug, sourceId: segmentSourceId } }
  }

  private buildOrganizations(
    row: Record<string, unknown>,
  ): IActivityData['member']['organizations'] {
    const website = (row.ORG_WEBSITE as string | null)?.trim() || null
    const domainAliases = (row.ORG_DOMAIN_ALIASES as string | null)?.trim() || null

    if (!website && !domainAliases) {
      return undefined
    }

    const displayName = (row.ACCOUNT_NAME as string | null)?.trim() || website

    if (this.isIndividualNoAccount(displayName)) {
      return [
        {
          displayName,
          source: OrganizationSource.COMMITTEES,
          identities: [
            {
              platform: PlatformType.COMMITTEES,
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
        platform: PlatformType.COMMITTEES,
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
            platform: PlatformType.COMMITTEES,
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
        source: OrganizationSource.COMMITTEES,
        identities,
      },
    ]
  }
}
