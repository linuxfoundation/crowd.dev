// processStream.ts content
import { IProcessStreamContext, ProcessStreamHandler } from '../../types'
import { getActivityLogs } from './api/getActivityLogs'
import { getGroupMembers } from './api/getGroupMembers'
import { getMessagesFromTopic } from './api/getMessagesFromTopic'
import { getPastGroupMembers } from './api/getPastGroupMembers'
import { getTopicsFromGroup } from './api/getTopicsFromGroup'
import {
  ActivityLog,
  GroupsioActivityLogsStreamMetadata,
  GroupsioGroupMembersStreamMetadata,
  GroupsioGroupStreamMetadata,
  GroupsioIntegrationSettings,
  GroupsioMemberJoinData,
  GroupsioMemberLeftData,
  GroupsioMessageData,
  GroupsioPastGroupMembersStreamMetadata,
  GroupsioPublishData,
  GroupsioPublishDataType,
  GroupsioStreamType,
  GroupsioTopicStreamMetadata,
  ListActivityLogs,
  ListMembers,
  ListMessages,
  ListPastMembers,
  ListTopics,
  MemberInfo,
  MemberInfoMinimal,
  PastMemberInfo,
} from './types'

const cacheMember = async (ctx: IProcessStreamContext, member: MemberInfo): Promise<void> => {
  const cacheKey = `${GroupsioStreamType.GROUP_MEMBERS}:${member.user_id}`
  const cache = ctx.cache

  // cache for 7 days
  await cache.set(cacheKey, JSON.stringify(member), 60 * 60 * 24 * 7)
}

const cachePastMember = async (
  ctx: IProcessStreamContext,
  pastMember: PastMemberInfo,
  group: string,
): Promise<void> => {
  const cacheKey = `${GroupsioStreamType.PAST_GROUP_MEMBERS}:${group}:${pastMember.member_info.user_id}`
  const cache = ctx.cache

  // cache for 7 days
  await cache.set(cacheKey, JSON.stringify(pastMember), 60 * 60 * 24 * 7)
}

const cacheGroupLastSync = async (
  ctx: IProcessStreamContext,
  groupname: string,
  lastSyncTS: string,
): Promise<void> => {
  const cacheKey = `${GroupsioStreamType.GROUP}:${groupname}`
  const cache = ctx.cache

  // cache for  7 days
  await cache.set(cacheKey, lastSyncTS, 60 * 60 * 24 * 7)
}

const cacheLatestGroupLastSync = async (
  ctx: IProcessStreamContext,
  groupname: string,
  lastSyncTS: string,
): Promise<void> => {
  const cacheKey = `latest:${GroupsioStreamType.GROUP}:${groupname}`
  const cache = ctx.cache

  // cache for  7 days
  await cache.set(cacheKey, lastSyncTS, 60 * 60 * 24 * 7)
}

const getMemberFromCache = async (
  ctx: IProcessStreamContext,
  userId: string,
): Promise<MemberInfo | undefined> => {
  const cacheKey = `${GroupsioStreamType.GROUP_MEMBERS}:${userId}`
  const cache = ctx.cache

  const cachedMember = await cache.get(cacheKey)

  if (!cachedMember) {
    return undefined
  }

  return JSON.parse(cachedMember)
}

const getPastMemberFromCache = async (
  ctx: IProcessStreamContext,
  userId: string,
  group: string,
): Promise<PastMemberInfo | undefined> => {
  const cacheKey = `${GroupsioStreamType.PAST_GROUP_MEMBERS}:${group}:${userId}`
  const cache = ctx.cache

  const cachedMember = await cache.get(cacheKey)

  if (!cachedMember) {
    return undefined
  }

  return JSON.parse(cachedMember)
}

const getGroupLastSyncFromCache = async (
  ctx: IProcessStreamContext,
  groupname: string,
): Promise<Date | undefined> => {
  const cacheKey = `${GroupsioStreamType.GROUP}:${groupname}`
  const cache = ctx.cache

  const lastSyncTS = await cache.get(cacheKey)

  if (!lastSyncTS) {
    return undefined
  }

  return new Date(lastSyncTS)
}

const getLatestGroupLastSyncFromCache = async (
  ctx: IProcessStreamContext,
  groupname: string,
): Promise<Date | undefined> => {
  const cacheKey = `latest:${GroupsioStreamType.GROUP}:${groupname}`
  const cache = ctx.cache

  const lastSyncTS = await cache.get(cacheKey)

  if (!lastSyncTS) {
    return undefined
  }

  return new Date(lastSyncTS)
}

const processGroupStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioGroupStreamMetadata
  const settings = ctx.integration.settings as GroupsioIntegrationSettings

  const response = (await getTopicsFromGroup(
    data.group,
    settings.token,
    ctx,
    data.page,
  )) as ListTopics

  const totalCount = response.total_count
  const thisPageCount = 10 // the default page size for groupsio is 10
  const pageNumber = data.groupsioPageNumber
  const isLastPage = pageNumber * thisPageCount >= totalCount ? true : false

  const onboarding = ctx.onboarding
  let lastGroupSyncTS = await getGroupLastSyncFromCache(ctx, data.group)
  if (lastGroupSyncTS === undefined) {
    lastGroupSyncTS = new Date(1990, 0, 1)
  }

  let reachedLastSync = false

  if (response.data !== null) {
    // get the updated date for the latest item in the response
    if (pageNumber == 1) {
      const latestLastUpdatedForCache = new Date(response.data[0].updated)
      await cacheLatestGroupLastSync(ctx, data.group, latestLastUpdatedForCache.toISOString())
    }

    // publishing topic streams
    for (const topic of response.data) {
      await ctx.publishStream<GroupsioTopicStreamMetadata>(
        `${GroupsioStreamType.TOPIC}:${topic.id}`,
        {
          group: data.group,
          topic,
          page: null,
        },
      )

      const topicUpdatedTS = new Date(topic.updated)

      if (
        (topicUpdatedTS.getDate() < lastGroupSyncTS.getDate() && !onboarding) ||
        isLastPage
        // !response?.next_page_token
      ) {
        reachedLastSync = true
        const latestLastGroupSyncTS = await getLatestGroupLastSyncFromCache(ctx, data.group)
        latestLastGroupSyncTS.setDate(latestLastGroupSyncTS.getDate() - 1)

        await cacheGroupLastSync(ctx, data.group, latestLastGroupSyncTS.toISOString())
        break
      }
    }
  }

  // processing next page stream
  if (onboarding || (!onboarding && !reachedLastSync)) {
    if (!isLastPage) {
      // if (response?.next_page_token) {
      await ctx.publishStream<GroupsioGroupStreamMetadata>(
        `${GroupsioStreamType.GROUP}:${data.group}-${response.next_page_token}`,
        {
          group: data.group,
          page: response.next_page_token.toString(),
          groupsioPageNumber: pageNumber + 1,
        },
      )
    }
  }
}

const processTopicStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioTopicStreamMetadata
  const settings = ctx.integration.settings as GroupsioIntegrationSettings

  const response = (await getMessagesFromTopic(
    data.topic.id.toString(),
    settings.token,
    ctx,
    data.page,
  )) as ListMessages

  // processing next page stream
  if (response?.next_page_token) {
    await ctx.publishStream<GroupsioTopicStreamMetadata>(
      `${GroupsioStreamType.TOPIC}:${data.topic.id}-${response.next_page_token}`,
      {
        group: data.group,
        topic: data.topic,
        page: response.next_page_token.toString(),
      },
    )
  }

  // publishing messages
  for (let i = 0; i < response.data.length; i++) {
    const message = response.data[i]
    const userId = message.user_id
    // getting member from cache
    // it should be there already because we process members first
    let member: MemberInfo | MemberInfoMinimal = await getMemberFromCache(ctx, userId.toString())

    if (!member) {
      // we didn't find a member in cache, so it's an anonymous user
      // we need to create a fake member object
      member = {
        user_id: userId,
        full_name: 'Anonymous User',
        email: 'anonymous+fake+email@groups.io',
        group_id: message.group_id,
      } as MemberInfoMinimal

      ctx.log.debug(
        { userId, messageId: message.id },
        'Member not found in cache, using anonymous member!',
      )
    }

    await ctx.processData<GroupsioPublishData<GroupsioMessageData>>({
      type: GroupsioPublishDataType.MESSAGE,
      data: {
        message,
        group: data.group,
        topic: data.topic,
        member,
        sourceParentId: i > 0 ? response.data[0].id.toString() : null,
      },
    })
  }
}

const processPastGroupMembersStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioPastGroupMembersStreamMetadata
  const settings = ctx.integration.settings as GroupsioIntegrationSettings

  let response: ListPastMembers
  try {
    response = await getPastGroupMembers(data.group, settings.token, ctx, data.page)
  } catch (error) {
    if (error.response.status === 400) {
      // no access for this endpoint for this group, just ignoring
      // we will go to activity logs
      response = null
    } else {
      throw error
    }
  }

  // publish members
  if (response?.data !== null && response?.data !== undefined) {
    for (const pastMember of response.data) {
      // we don't process expired member events because these are
      // approvals that were not approved on time
      if (pastMember.action === 'expired_member') {
        continue
      }
      // caching member
      await cachePastMember(ctx, pastMember, data.group)
      // publishing member
      await ctx.processData<GroupsioPublishData<GroupsioMemberLeftData>>({
        type: GroupsioPublishDataType.MEMBER_LEFT,
        data: {
          member: pastMember.member_info,
          group: data.group,
          leftAt: new Date(pastMember.created).toISOString(),
        },
      })
    }
  }

  // processing next page stream
  if (response?.next_page_token) {
    await ctx.publishStream<GroupsioPastGroupMembersStreamMetadata>(
      `${GroupsioStreamType.PAST_GROUP_MEMBERS}:${data.group}-${response.next_page_token}`,
      {
        group: data.group,
        page: response.next_page_token.toString(),
      },
    )
  } else {
    // publish activity logs streams to get member_join for historic members
    // i.e past members
    await ctx.publishStream<GroupsioActivityLogsStreamMetadata>(
      `${GroupsioStreamType.ACTIVITY_LOGS}:${data.group}`,
      {
        group: data.group,
        page: null,
      },
    )
  }
}

const processGroupMembersStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioGroupMembersStreamMetadata
  const settings = ctx.integration.settings as GroupsioIntegrationSettings

  const response = (await getGroupMembers(
    data.group,
    settings.token,
    ctx,
    data.page,
  )) as ListMembers

  // publish members
  if (response?.data !== null) {
    for (const member of response.data) {
      // caching member
      await cacheMember(ctx, member)
      // publishing member
      await ctx.processData<GroupsioPublishData<GroupsioMemberJoinData>>({
        type: GroupsioPublishDataType.MEMBER_JOIN,
        data: {
          member,
          group: data.group,
          joinedAt: new Date(member.created).toISOString(),
        },
      })
    }
  }

  // processing next page stream
  if (response?.next_page_token) {
    await ctx.publishStream<GroupsioGroupMembersStreamMetadata>(
      `${GroupsioStreamType.GROUP_MEMBERS}:${data.group}-${response.next_page_token}`,
      {
        group: data.group,
        page: response.next_page_token.toString(),
      },
    )
  } else {
    // this is the last page, so we can publish the group streams
    await ctx.publishStream<GroupsioGroupStreamMetadata>(
      `${GroupsioStreamType.GROUP}:${data.group}`,
      {
        group: data.group,
        page: null,
        groupsioPageNumber: 1,
      },
    )
  }
}

const parseActivityLog = async (ctx: IProcessStreamContext, activity: ActivityLog) => {
  interface ActivityObj {
    user_id: string
    user_email: string
  }
  const data = ctx.stream.data as GroupsioGroupMembersStreamMetadata

  // regexp for multiple ways a member can join a group in activity logs
  const MEMBER_ACCEPT_INVITATION = new RegExp(
    '<a.*href=\\".*\\/memberbyuid\\/(\\d+)\\">(.*&lt;)*([\\w\\.-]+@[\\w-]+(\\.+[\\w-]{2,4}){1,3}).*<\\/a>\\s*accepted\\s*invitation.*$',
  )
  const MEMBER_DIRECT_ADD = new RegExp(
    '<a.*href=\\".*\\/memberbyuid\\/\\d+\\">.+<\\/a>\\s*added\\s*<a.*href=\\".*\\/memberbyuid\\/(\\d+)\\">(.*&lt;)*([\\w\\.-]+@[\\w-]+(\\.+[\\w-]{2,4}){1,3}).*<\\/a>.*$',
  )
  const MEMBER_SUBSCRIBE = new RegExp(
    '<a.*href=\\".*\\/memberbyuid\\/(\\d+)\\">(.*&lt;)*([\\w\\.-]+@[\\w-]+(\\.+[\\w-]{2,4}){1,3}).*<\\/a>\\s*subscribed.*$',
  )
  const MEMBER_GROUP_CREATE = new RegExp(
    '<a.*href=\\".*\\/memberbyuid\\/(\\d+)\\">.*<\\/a>\\s*created\\s*the\\s*group.*$',
  )
  const MEMBER_APPROVE_MEMBERSHIP = new RegExp(
    `<a.*>.+<\\/a>\\s*approved\\s*<a.*href=\\".*\\/memberbyuid\\/(\\d+)\\">(.*&lt;)*([\\w\\.-]+@[\\w-]+(\\.+[\\w-]{2,4}){1,3}).*<\\/a>\\s*'s\\s*membership.*$`,
  )

  if (
    MEMBER_ACCEPT_INVITATION.test(activity.entry) ||
    MEMBER_DIRECT_ADD.test(activity.entry) ||
    MEMBER_GROUP_CREATE.test(activity.entry) ||
    MEMBER_SUBSCRIBE.test(activity.entry) ||
    MEMBER_APPROVE_MEMBERSHIP.test(activity.entry)
  ) {
    const thisActivity = {} as ActivityObj
    const joinByInvitation = MEMBER_ACCEPT_INVITATION.test(activity.entry)
    const joinBySubscribe = MEMBER_SUBSCRIBE.test(activity.entry)
    const joinByAdd = MEMBER_DIRECT_ADD.test(activity.entry)
    const joinByGroupCreate = MEMBER_GROUP_CREATE.test(activity.entry)
    const joinByApproveMembership = MEMBER_APPROVE_MEMBERSHIP.test(activity.entry)
    let memberJoinMatch: RegExpExecArray | null

    if (joinByAdd) {
      memberJoinMatch = MEMBER_DIRECT_ADD.exec(activity.entry)
      thisActivity.user_email = memberJoinMatch[3]
    } else if (joinByInvitation) {
      memberJoinMatch = MEMBER_ACCEPT_INVITATION.exec(activity.entry)
      thisActivity.user_email = memberJoinMatch[3]
    } else if (joinByGroupCreate) {
      memberJoinMatch = MEMBER_GROUP_CREATE.exec(activity.entry)
    } else if (joinBySubscribe) {
      memberJoinMatch = MEMBER_SUBSCRIBE.exec(activity.entry)
      thisActivity.user_email = memberJoinMatch[3]
    } else if (joinByApproveMembership) {
      memberJoinMatch = MEMBER_APPROVE_MEMBERSHIP.exec(activity.entry)
      thisActivity.user_email = memberJoinMatch[3]
    }

    if (memberJoinMatch) {
      thisActivity.user_id = memberJoinMatch[1]
    } else {
      return
    }

    const pastMember: PastMemberInfo = await getPastMemberFromCache(
      ctx,
      thisActivity.user_id.toString(),
      data.group,
    )

    // we are only getting member join from activity logs for past members
    if (pastMember) {
      // publishing member
      await ctx.processData<GroupsioPublishData<GroupsioMemberJoinData>>({
        type: GroupsioPublishDataType.MEMBER_JOIN,
        data: {
          member: pastMember.member_info,
          group: data.group,
          joinedAt: new Date(activity.created).toISOString(),
        },
      })
    }
  }
}
const processActivityLogs: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioActivityLogsStreamMetadata
  const settings = ctx.integration.settings as GroupsioIntegrationSettings

  const response = (await getActivityLogs(
    data.group,
    settings.token,
    ctx,
    data.page,
  )) as ListActivityLogs

  for (const log of response.data) {
    await parseActivityLog(ctx, log)
  }

  if (response?.next_page_token) {
    await ctx.publishStream<GroupsioActivityLogsStreamMetadata>(
      `${GroupsioStreamType.ACTIVITY_LOGS}:${data.group}-${response.next_page_token}`,
      {
        group: data.group,
        page: response.next_page_token.toString(),
      },
    )
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  const streamIdentifier = ctx.stream.identifier

  const streamType = streamIdentifier.split(':')[0]

  switch (streamType) {
    case GroupsioStreamType.GROUP:
      await processGroupStream(ctx)
      break
    case GroupsioStreamType.TOPIC:
      await processTopicStream(ctx)
      break
    case GroupsioStreamType.GROUP_MEMBERS:
      await processGroupMembersStream(ctx)
      break
    case GroupsioStreamType.PAST_GROUP_MEMBERS:
      await processPastGroupMembersStream(ctx)
      break
    case GroupsioStreamType.ACTIVITY_LOGS:
      await processActivityLogs(ctx)
      break
    default:
      await ctx.abortRunWithError(`Unknown stream type: ${streamType}`)
      break
  }
}

export default handler
