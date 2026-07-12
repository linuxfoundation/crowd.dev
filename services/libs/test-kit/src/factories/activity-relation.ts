import { faker } from '@faker-js/faker'

import { generateUUIDv1 } from '@crowd/common'
import { createOrUpdateRelations } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import { DEFAULT_ACTIVITY_TYPE_SETTINGS } from '@crowd/integrations/src/integrations/activityTypes'
import {
  type ActivityRelationDbInsert,
  type MemberIdentityDbRow,
  type MemberOrganizationDbRow,
  PlatformType,
} from '@crowd/types'

import { withDefaults } from './defaults'

const DEFAULT_COUNT = 100

interface GenerateActivityRelationsInput {
  memberId: string
  segmentId: string
  identities: MemberIdentityDbRow[]
  timestamp: {
    from: string
    to: string
  }
  count?: number
  memberOrganizations?: MemberOrganizationDbRow[]
}

export const withActivityRelationDefaults = (
  data: Partial<ActivityRelationDbInsert>[],
): ActivityRelationDbInsert[] =>
  withDefaults<ActivityRelationDbInsert>({
    activityId: () => generateUUIDv1(),
    sourceId: () => generateUUIDv1(),
  })(data)

/** Build N activityRelation rows from member context. */
export function generateActivityRelations(
  input: GenerateActivityRelationsInput,
): ActivityRelationDbInsert[] {
  const {
    memberId,
    segmentId,
    identities,
    timestamp,
    memberOrganizations,
    count = DEFAULT_COUNT,
  } = input

  if (identities.length === 0) {
    throw new Error('generateActivityRelations requires at least one identity')
  }

  if (count < 1) {
    throw new Error('generateActivityRelations count must be >= 1')
  }

  const fromMs = new Date(timestamp.from).getTime()
  const toMs = new Date(timestamp.to).getTime()

  if (Number.isNaN(fromMs) || Number.isNaN(toMs)) {
    throw new Error('generateActivityRelations timestamp.from/to must be valid dates')
  }

  if (fromMs > toMs) {
    throw new Error('generateActivityRelations timestamp.from must be <= timestamp.to')
  }

  // Same channel for all rows of a given platform.
  const channelByPlatform = new Map<PlatformType, string>()

  return Array.from({ length: count }, (_, index) => {
    const identity = identities[index % identities.length]
    const platform = identity.platform as PlatformType

    // Evenly spaced across [from, to].
    const activityTimestamp =
      count === 1 ? new Date(fromMs) : new Date(fromMs + ((toMs - fromMs) * index) / (count - 1))

    let channel = channelByPlatform.get(platform)
    if (!channel) {
      channel = channelFor(platform)
      channelByPlatform.set(platform, channel)
    }

    return {
      activityId: generateUUIDv1(),
      memberId,
      segmentId,
      platform,
      username: identity.value,
      sourceId: generateUUIDv1(),
      type: faker.helpers.arrayElement(activityTypesFor(platform)),
      timestamp: activityTimestamp.toISOString(),
      channel,
      organizationId: resolveOrganizationId(activityTimestamp, memberOrganizations),
      score: faker.number.int({ min: 0, max: 10 }),
    }
  })
}

/** Insert activityRelation rows. */
export async function createActivityRelations(
  qx: QueryExecutor,
  data: ActivityRelationDbInsert[],
): Promise<ActivityRelationDbInsert[]> {
  if (data.length === 0) {
    return []
  }

  await createOrUpdateRelations(qx, data as Parameters<typeof createOrUpdateRelations>[1], true)

  return data
}

function activityTypesFor(platform: PlatformType): string[] {
  // Treat github-nango as github for activity types.
  const key = platform === PlatformType.GITHUB_NANGO ? PlatformType.GITHUB : platform

  const types = Object.keys(DEFAULT_ACTIVITY_TYPE_SETTINGS[key] ?? {})

  return types.length > 0 ? types : ['activity']
}

function channelFor(platform: PlatformType): string {
  const org = faker.internet.domainWord()
  const repo = faker.internet.domainWord()

  switch (platform) {
    case PlatformType.GITHUB:
    case PlatformType.GITHUB_NANGO:
    case PlatformType.GIT:
      return `https://github.com/${org}/${repo}`

    case PlatformType.GITLAB:
      return `https://gitlab.com/${org}/${repo}`

    case PlatformType.DISCORD:
    case PlatformType.SLACK:
      return `#${faker.word.noun()}`

    default:
      return `${org}/${repo}`
  }
}

/** Pick organizationId for a timestamp from the given stints. */
function resolveOrganizationId(timestamp: Date, stints?: MemberOrganizationDbRow[]): string | null {
  if (!stints?.length) {
    return null
  }

  let fallback: string | null = null

  for (const stint of stints) {
    // No dates → keep as fallback; dated match returns immediately.
    if (!stint.dateStart && !stint.dateEnd) {
      fallback ??= stint.organizationId
      continue
    }

    const start = stint.dateStart ? new Date(stint.dateStart) : undefined
    const end = stint.dateEnd ? new Date(stint.dateEnd) : undefined

    if ((!start || timestamp >= start) && (!end || timestamp <= end)) {
      return stint.organizationId
    }
  }

  return fallback
}
