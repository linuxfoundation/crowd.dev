import {
  IActivityData,
  IMemberData,
  IntegrationResultType,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { ProcessDataHandler } from '../../types'

import { TWITTER_GRID } from './grid'
import { TwitterActivityType, TwitterPublishData, TwitterStreamType } from './types'

const processTweetsWithMentions: ProcessDataHandler = async (ctx) => {
  const metadata = ctx.data as TwitterPublishData
  const data = metadata.data

  const member: IMemberData = {
    identities: [
      {
        value: data.member.username,
        type: MemberIdentityType.USERNAME,
        platform: PlatformType.TWITTER,
        sourceId: data.member.id,
        verified: true,
        verifiedBy: PlatformType.TWITTER,
      },
    ],
    attributes: {
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.TWITTER]: data.member.id,
      },
      [MemberAttributeName.URL]: {
        [PlatformType.TWITTER]: `https://twitter.com/${data.member.username}`,
      },
      ...(data.member.profile_image_url && {
        [MemberAttributeName.AVATAR_URL]: {
          [PlatformType.TWITTER]: data.member.profile_image_url,
        },
      }),
      ...(data.member.location && {
        [MemberAttributeName.LOCATION]: {
          [PlatformType.TWITTER]: data.member.location,
        },
      }),
      ...(data.member.description && {
        [MemberAttributeName.BIO]: {
          [PlatformType.TWITTER]: data.member.description,
        },
      }),
    },
    reach: {
      [PlatformType.TWITTER]: data.member.public_metrics.followers_count,
    },
  }

  const out: IActivityData = {
    member,
    type: TwitterActivityType.MENTION,
    sourceId: data.id,
    timestamp: new Date(Date.parse(data.created_at)).toISOString(),
    body: data.text ? data.text : '',
    url: `https://twitter.com/i/status/${data.id}`,
    attributes: {
      attachments: data.attachments ? data.attachments : [],
      entities: data.entities ? data.entities : [],
    },
    score: TWITTER_GRID[TwitterActivityType.HASHTAG].score,
  }

  await ctx.publishActivity(out)
}

const processTweetsWithHashtags: ProcessDataHandler = async (ctx) => {
  const metadata = ctx.data as TwitterPublishData
  const data = metadata.data

  const member: IMemberData = {
    identities: [
      {
        value: data.member.username,
        type: MemberIdentityType.USERNAME,
        platform: PlatformType.TWITTER,
        sourceId: data.member.id,
        verified: true,
        verifiedBy: PlatformType.TWITTER,
      },
    ],
    attributes: {
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.TWITTER]: data.member.id,
      },
      [MemberAttributeName.URL]: {
        [PlatformType.TWITTER]: `https://twitter.com/${data.member.username}`,
      },
      ...(data.member.profile_image_url && {
        [MemberAttributeName.AVATAR_URL]: {
          [PlatformType.TWITTER]: data.member.profile_image_url,
        },
      }),
      ...(data.member.location && {
        [MemberAttributeName.LOCATION]: {
          [PlatformType.TWITTER]: data.member.location,
        },
      }),
      ...(data.member.description && {
        [MemberAttributeName.BIO]: {
          [PlatformType.TWITTER]: data.member.description,
        },
      }),
    },
    reach: {
      [PlatformType.TWITTER]: data.member.public_metrics.followers_count,
    },
  }

  const out: IActivityData = {
    member,
    type: TwitterActivityType.HASHTAG,
    sourceId: data.id,
    timestamp: new Date(Date.parse(data.created_at)).toISOString(),
    body: data.text ? data.text : '',
    url: `https://twitter.com/i/status/${data.id}`,
    attributes: {
      attachments: data.attachments ? data.attachments : [],
      entities: data.entities ? data.entities : [],
      hashtag: metadata.hashtag,
    },
    score: TWITTER_GRID[TwitterActivityType.HASHTAG].score,
  }

  await ctx.publishActivity(out)
}

const processMemberReachUpdate: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as TwitterPublishData

  const member: IMemberData = {
    identities: [
      {
        value: data.member.username,
        type: MemberIdentityType.USERNAME,
        platform: PlatformType.TWITTER,
        sourceId: data.member.id,
        verified: true,
        verifiedBy: PlatformType.TWITTER,
      },
    ],
    attributes: {
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.TWITTER]: data.member.id,
      },
      [MemberAttributeName.URL]: {
        [PlatformType.TWITTER]: `https://twitter.com/${data.member.username}`,
      },
      ...(data.member.profile_image_url && {
        [MemberAttributeName.AVATAR_URL]: {
          [PlatformType.TWITTER]: data.member.profile_image_url,
        },
      }),
      ...(data.member.location && {
        [MemberAttributeName.LOCATION]: {
          [PlatformType.TWITTER]: data.member.location,
        },
      }),
      ...(data.member.description && {
        [MemberAttributeName.BIO]: {
          [PlatformType.TWITTER]: data.member.description,
        },
      }),
    },
    reach: {
      [PlatformType.TWITTER]: data.member.public_metrics.followers_count,
    },
  }

  await ctx.publishCustom(member, IntegrationResultType.TWITTER_MEMBER_REACH)
}

const handler: ProcessDataHandler = async (ctx) => {
  const metadata = ctx.data as TwitterPublishData
  const type = metadata.type

  switch (type) {
    case TwitterStreamType.MENTIONS:
      await processTweetsWithMentions(ctx)
      break
    case TwitterStreamType.HASHTAG:
      await processTweetsWithHashtags(ctx)
      break
    case TwitterStreamType.REACH:
      await processMemberReachUpdate(ctx)
      break
    default:
      ctx.abortWithError('Uknown API Data stream type', { data: ctx.data })
  }
}

export default handler
