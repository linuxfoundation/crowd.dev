import type { IMemberOrganization } from '@crowd/types'

export type MemberOrganizationWithOverrides = IMemberOrganization & {
  isPrimaryWorkExperience: boolean
  memberCount: number
}

export type TimelineItem = {
  dateStart: string
  dateEnd: string | null
  organizationId: string | null
  segmentId?: string
  skipManualAffiliationSegments?: boolean

  /**
   * Routes activities by their email domain so timeline passes never overwrite each other.
   * Claims activities that match this specific organization domain.
   */
  matchEmailDomain?: string

  /**
   * Excludes activities belonging to these specific email domains from being claimed.
   */
  excludeEmailDomains?: string[]
}
