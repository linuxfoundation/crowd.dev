import sanitizeHtml from 'sanitize-html'

import {
  IActivityData,
  IMemberData,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { IProcessDataContext, ProcessDataHandler } from '../../types'

import { SLACK_GRID } from './grid'
import { ISlackAPIData, SlackActivityType, SlackMember, SlackMessage } from './types'

/**
 * Get the URL for a Slack message
 * @param stream Stream we are parsing
 * @param pipelineData Pipeline data
 * @param record Message record
 * @returns Return the url: workspaceUrl + channelUrl + messageUrl
 */
function getUrl(channelId: string, teamUrl: string, record: SlackMessage) {
  return `${teamUrl}archives/${channelId}/p${record.ts.replace('.', '')}`
}

function parseMember(record: SlackMember): IMemberData {
  const member: IMemberData = {
    displayName: record.profile.real_name,
    identities: [
      {
        platform: PlatformType.SLACK,
        value: record.name,
        type: MemberIdentityType.USERNAME,
        sourceId: record.id,
        verified: true,
        verifiedBy: PlatformType.SLACK,
      },
    ],
    attributes: {
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.SLACK]: record.id,
      },
      ...(record.profile.image_72 && {
        [MemberAttributeName.AVATAR_URL]: {
          [PlatformType.SLACK]: record.profile.image_72,
        },
      }),
      ...(record.tz_label && {
        [MemberAttributeName.TIMEZONE]: {
          [PlatformType.SLACK]: record.tz_label,
        },
      }),
      ...(record.profile.title && {
        [MemberAttributeName.JOB_TITLE]: {
          [PlatformType.SLACK]: record.profile.title,
        },
      }),
    },
  }

  if (record.profile.email) {
    member.identities.push({
      platform: PlatformType.SLACK,
      value: record.profile.email,
      type: MemberIdentityType.EMAIL,
      sourceId: record.id,
      verified: true,
      verifiedBy: PlatformType.SLACK,
    })
  }

  return member
}

const parseChannel = async (ctx: IProcessDataContext) => {
  const data = ctx.data as ISlackAPIData

  const message = data.message
  const member = data.member
  const channelId = data.channelId

  if (member !== undefined) {
    let body = message.text // it's already cleaned from mentions
    let activityType
    let score
    let sourceId
    if (message.subtype === 'channel_join') {
      activityType = 'channel_joined'
      score = SLACK_GRID[SlackActivityType.JOINED_CHANNEL].score
      body = undefined
      sourceId = message.user
    } else {
      activityType = 'message'
      score = SLACK_GRID[SlackActivityType.MESSAGE].score
      sourceId = message.ts
    }

    const activity: IActivityData = {
      type: activityType,
      sourceId,
      sourceParentId: '',
      timestamp: new Date(parseInt(message.ts, 10) * 1000).toISOString(),
      body,
      url: getUrl(channelId, data.base.teamUrl, message),
      channel: data.base.channelsInfo[channelId].name,
      attributes: {
        thread: false,
        reactions: message.reactions ? message.reactions : [],
        attachments: message.attachments ? message.attachments : [],
      },
      score,
      member: parseMember(member),
    }

    await ctx.publishActivity(activity)
  }
}

const parseThreads = async (ctx: IProcessDataContext) => {
  const data = ctx.data as ISlackAPIData

  const message = data.message
  const member = data.member

  if (member !== undefined) {
    const activity: IActivityData = {
      type: 'message',
      sourceId: message.ts,
      sourceParentId: data.threadId,
      timestamp: new Date(parseInt(message.ts, 10) * 1000).toISOString(),
      body: message.text ? sanitizeHtml(message.text) : '',
      url: getUrl(data.channelId, data.base.teamUrl, message),
      channel: data.channel,
      attributes: {
        thread: {
          body: sanitizeHtml(data.placeholder),
          id: data.threadId,
        },
        reactions: message.reactions ? message.reactions : [],
        attachments: message.attachments ? message.attachments : [],
      },
      score: SLACK_GRID[SlackActivityType.MESSAGE].score,
      member: parseMember(member),
    }

    await ctx.publishActivity(activity)
  }
}

const parseMembers = async (ctx: IProcessDataContext) => {
  const data = ctx.data as ISlackAPIData

  const member = data.member

  if (member !== undefined && !member.is_bot) {
    const activity: IActivityData = {
      type: 'channel_joined',
      sourceId: member.id,
      timestamp: new Date('1970-01-01T00:00:00+00:00').toISOString(),
      body: undefined,
      attributes: {
        thread: false,
      },
      score: SLACK_GRID[SlackActivityType.JOINED_CHANNEL].score,
      member: parseMember(member),
    }

    await ctx.publishActivity(activity)
  }
}

const handler: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as ISlackAPIData
  switch (data.type) {
    case 'channel':
      await parseChannel(ctx)
      break
    case 'threads':
      await parseThreads(ctx)
      break
    case 'members':
      await parseMembers(ctx)
      break
    default:
      ctx.log.warn(`Unknown data type: ${data.type}`)
      await ctx.abortRunWithError(`Unknown data type: ${data.type}`)
      break
  }
}

export default handler
