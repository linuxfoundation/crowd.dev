import { proxyActivities } from '@temporalio/workflow'

import { IMember, MemberIdentityType } from '@crowd/types'

import * as activities from '../../activities'
import { IGetEnrichmentDataArgs } from '../../sources/lfid/types'

const { refreshToken, getEnrichmentLFAuth0 } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 seconds',
})

export async function getEnrichmentData(args: IGetEnrichmentDataArgs): Promise<void> {
  const token = await refreshToken()

  const mem: IMember = {
    id: '123',
    segmentId: null,
    attributes: {},
    score: 0,
    joinedAt: null,
    createdAt: null,
    manuallyCreated: false,
    numberOfOpenSourceContributions: 0,
    activeOn: [],
    activityCount: 0,
    lastActive: null,
    averageSentiment: 0,
    enrichedBy: [],
    identities: [],
    organizations: [],
    toMergeIds: [],
    noMergeIds: [],
    lastActivity: null,
  }

  if (args.email) {
    mem.identities.push({
      platform: 'github',
      type: MemberIdentityType.EMAIL,
      value: args.email,
      verified: true,
      source: 'enrichment',
    })
  }

  if (args.lfid) {
    mem.identities.push({
      platform: 'lfid',
      type: MemberIdentityType.USERNAME,
      value: args.lfid,
      verified: true,
      source: 'enrichment',
    })
  }

  if (args.githubSourceId) {
    mem.identities.push({
      platform: 'github',
      type: MemberIdentityType.USERNAME,
      sourceId: args.githubSourceId,
      value: 'some-value',
      verified: true,
      source: 'enrichment',
    })
  }

  if (args.linkedinSourceId) {
    mem.identities.push({
      platform: 'linkedin',
      type: MemberIdentityType.USERNAME,
      sourceId: args.linkedinSourceId,
      value: 'some-value',
      verified: true,
      source: 'enrichment',
    })
  }

  const data = await getEnrichmentLFAuth0(token, mem)
  console.log(data)
  console.log(data.identities.find((i) => i.provider === 'github')?.profileData)
}
