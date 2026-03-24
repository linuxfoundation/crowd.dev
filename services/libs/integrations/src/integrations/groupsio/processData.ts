// processData.ts content
import sanitizeHtml from 'sanitize-html'

import { IActivityData, IMemberData, MemberIdentityType, PlatformType } from '@crowd/types'

import { ProcessDataHandler } from '../../types'
import { roundToNearestMinute } from '../utils'

import { Groupsio_GRID } from './grid'
import {
  GroupsioActivityType,
  GroupsioMemberJoinData,
  GroupsioMemberLeftData,
  GroupsioMessageData,
  GroupsioPublishData,
  GroupsioPublishDataType,
  MemberInfo,
} from './types'

const processMemberJoin: ProcessDataHandler = async (ctx) => {
  const metaData = ctx.data as GroupsioPublishData<GroupsioMemberJoinData>
  const data = metaData.data
  const memberData = data.member as MemberInfo

  const member: IMemberData = {
    displayName: memberData.full_name,
    identities: [
      {
        sourceId: memberData.user_id.toString(),
        platform: PlatformType.GROUPSIO,
        value: memberData.email,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GROUPSIO,
      },
      {
        sourceId: memberData.user_id.toString(),
        platform: PlatformType.GROUPSIO,
        value: memberData.email,
        type: MemberIdentityType.EMAIL,
        verified: true,
        verifiedBy: PlatformType.GROUPSIO,
      },
    ],
  }

  const roundToMinJoinedAt = roundToNearestMinute(data.joinedAt)
  const activity: IActivityData = {
    type: GroupsioActivityType.MEMBER_JOIN,
    member,
    channel: data.group,
    timestamp: data.joinedAt,
    sourceId: `join-${memberData.user_id}-${memberData.group_id}-${roundToMinJoinedAt}`,
    score: Groupsio_GRID[GroupsioActivityType.MEMBER_JOIN].score,
    attributes: {
      userStatus: memberData.user_status,
    },
  }

  await ctx.publishActivity(activity)
}

const processMessage: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as GroupsioPublishData<GroupsioMessageData>
  const messageData = data.data
  const memberData = messageData.member

  const member: IMemberData = {
    displayName: memberData.full_name,
    identities: [
      {
        sourceId: memberData.user_id.toString(),
        platform: PlatformType.GROUPSIO,
        value: memberData.email,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GROUPSIO,
      },
      {
        sourceId: memberData.user_id.toString(),
        platform: PlatformType.GROUPSIO,
        value: memberData.email,
        type: MemberIdentityType.EMAIL,
        verified: true,
        verifiedBy: PlatformType.GROUPSIO,
      },
    ],
  }

  const activity: IActivityData = {
    type: GroupsioActivityType.MESSAGE,
    member,
    channel: messageData.group,
    timestamp: new Date(messageData.message.created).toISOString(),
    sourceId: `${messageData.message.id}`,
    score: Groupsio_GRID[GroupsioActivityType.MESSAGE].score,
    body: sanitizeHtml(messageData.message.body),
    title: messageData.topic.subject,
    ...(messageData.sourceParentId && {
      sourceParentId: messageData.sourceParentId,
    }),
  }

  await ctx.publishActivity(activity)
}

const processMemberLeft: ProcessDataHandler = async (ctx) => {
  const metaData = ctx.data as GroupsioPublishData<GroupsioMemberLeftData>
  const data = metaData.data
  const memberData = data.member as MemberInfo

  const member: IMemberData = {
    displayName: memberData.full_name,
    identities: [
      {
        sourceId: memberData.user_id.toString(),
        platform: PlatformType.GROUPSIO,
        value: memberData.email,
        type: MemberIdentityType.USERNAME,
        verified: true,
        verifiedBy: PlatformType.GROUPSIO,
      },
      {
        sourceId: memberData.user_id.toString(),
        platform: PlatformType.GROUPSIO,
        value: memberData.email,
        type: MemberIdentityType.EMAIL,
        verified: true,
        verifiedBy: PlatformType.GROUPSIO,
      },
    ],
  }

  const roundToMinLeftAt = roundToNearestMinute(data.leftAt)
  const activity: IActivityData = {
    type: GroupsioActivityType.MEMBER_LEAVE,
    member,
    channel: data.group,
    timestamp: data.leftAt,
    sourceId: `left-${memberData.user_id}-${memberData.group_id}-${roundToMinLeftAt}`,
    score: Groupsio_GRID[GroupsioActivityType.MEMBER_LEAVE].score,
  }

  await ctx.publishActivity(activity)
}

const handler: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as GroupsioPublishData<unknown>

  const type = data.type

  switch (type) {
    case GroupsioPublishDataType.MEMBER_JOIN:
      await processMemberJoin(ctx)
      break
    case GroupsioPublishDataType.MESSAGE:
      await processMessage(ctx)
      break
    case GroupsioPublishDataType.MEMBER_LEFT:
      await processMemberLeft(ctx)
      break
    default:
      await ctx.abortRunWithError(`Unknown publish data type: ${type}`)
  }
}

export default handler
