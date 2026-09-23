import { IProcessStreamContext, ProcessStreamHandler } from '../../types'
import getChannels from './api/getChannels'
import getMember from './api/getMember'
import getMembers from './api/getMembers'
import getMessages from './api/getMessages'
import getMessagesInThreads from './api/getMessagesInThreads'
import getTeam from './api/getTeam'
import {
  ISlackAPIData,
  ISlackChannelStreamData,
  ISlackMemberStreamData,
  ISlackRootStreamData,
  ISlackStreamBase,
  ISlackThreadStreamData,
  SlackMember,
  SlackMessage,
  SlackStreamType,
} from './types'

async function removeMentions(text: string, ctx: IProcessStreamContext): Promise<string> {
  const regex = /<@!?[^>]*>/
  const globalRegex = /<@!?[^>]*>/g
  const matches = text.match(globalRegex)
  if (matches) {
    for (let match of matches) {
      match = match.replace('<', '').replace('>', '').replace('@', '').replace('!', '')

      const user = await getSlackMember(match, ctx)
      const username = user ? user.name : 'mention'
      text = text.replace(regex, `@${username}`)
    }
  }

  return text
}

async function getSlackMember(
  userId: string,
  ctx: IProcessStreamContext,
): Promise<SlackMember | undefined> {
  const membersCache = ctx.cache
  const metadata = ctx.stream.data as ISlackStreamBase

  const prefix = (val: string) => `slack-member:${val}`

  const cached = await membersCache.get(prefix(userId))
  if (cached) {
    if (cached === 'null') {
      return undefined
    }

    return JSON.parse(cached)
  }
  const result = await getMember(
    {
      token: metadata.token,
      userId,
    },
    ctx,
  )

  const member = result.records

  if (member) {
    await membersCache.set(prefix(userId), JSON.stringify(member), 24 * 60 * 60)

    return member
  }

  await membersCache.set(prefix(userId), 'null', 24 * 60 * 60)
  return undefined
}

const processRootStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as ISlackRootStreamData
  const token = metadata.token
  const channels = metadata.channels ? metadata.channels : []

  let channelsFromSlackAPI = await getChannels({ token }, ctx)

  channelsFromSlackAPI = channelsFromSlackAPI.map((c) => {
    if (channels.filter((a) => a.id === c.id).length <= 0) {
      return { ...c, new: true }
    }
    return c
  })

  await ctx.updateIntegrationSettings({
    ...(ctx.integration.settings as object),
    channels: channelsFromSlackAPI,
  })

  const team = await getTeam({ token }, ctx)
  const teamUrl = team.url

  const channelsInfo = channelsFromSlackAPI.reduce((acc, channel) => {
    acc[channel.id] = {
      name: channel.name,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      new: !!(channel as any).new,
    }
    return acc
  }, {})

  if (ctx.onboarding) {
    await ctx.publishStream<ISlackMemberStreamData>(`${SlackStreamType.MEMBERS}:`, {
      page: '',
      token,
      channelsInfo,
      teamUrl,
      team,
      channels: channelsFromSlackAPI,
    })
  }

  for (const c of channelsFromSlackAPI) {
    await ctx.publishStream<ISlackChannelStreamData>(`${SlackStreamType.CHANNEL}:${c.id}`, {
      channelId: c.id,
      page: '',
      general: c.general,
      token,
      channelsInfo,
      teamUrl,
      team,
      channels: channelsFromSlackAPI,
    })
  }
}

const processChannelStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as ISlackChannelStreamData

  const result = await getMessages(
    {
      channelId: metadata.channelId,
      page: metadata.page,
      perPage: 200,
      token: metadata.token,
      new: metadata.channelsInfo[metadata.channelId].new,
    },
    ctx,
  )

  const nextPage = result.nextPage

  // publishing next page stream
  if (nextPage) {
    await ctx.publishStream<ISlackChannelStreamData>(
      `${SlackStreamType.CHANNEL}:${metadata.channelId}:${nextPage}`,
      {
        page: nextPage,
        channelId: metadata.channelId,
        general: metadata.general,
        token: metadata.token,
        channelsInfo: metadata.channelsInfo,
        teamUrl: metadata.teamUrl,
        team: metadata.team,
        channels: metadata.channels,
      },
    )
  }

  const messages = result.records as SlackMessage[]

  // publishing new streams for each thread
  while (messages.length > 0) {
    const message = messages.shift()
    const userId = message.user
    const member = await getSlackMember(userId, ctx)

    // if member is undefined we don't publish the activity and thread stream
    if (member !== undefined) {
      //  removing methions here instead of in the processData handler because we need to do API calls
      message.text = message.text ? await removeMentions(message.text, ctx) : ''

      const thread_ts = message.thread_ts

      if (thread_ts) {
        await ctx.publishStream<ISlackThreadStreamData>(
          `${SlackStreamType.THREADS}:${metadata.channelId}:${thread_ts}`,
          {
            page: '',
            threadId: thread_ts,
            channel: metadata.channelsInfo[metadata.channelId].name,
            channelId: metadata.channelId,
            new: metadata.channelsInfo[metadata.channelId].new,
            token: metadata.token,
            channelsInfo: metadata.channelsInfo,
            teamUrl: metadata.teamUrl,
            team: metadata.team,
            channels: metadata.channels,
            placeholder: message.text,
          },
        )
      }

      await ctx.processData<ISlackAPIData>({
        type: 'channel',
        message,
        member,
        channelId: metadata.channelId,
        base: {
          token: metadata.token,
          channelsInfo: metadata.channelsInfo,
          teamUrl: metadata.teamUrl,
          team: metadata.team,
          channels: metadata.channels,
        },
      })
    }
  }
}

const processThreadStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as ISlackThreadStreamData
  const result = await getMessagesInThreads(
    {
      token: metadata.token,
      channelId: metadata.channelId,
      page: metadata.page,
      perPage: 200,
      threadId: metadata.threadId,
      new: metadata.new,
    },
    ctx,
  )

  const nextPage = result.nextPage

  // publishing next page stream
  if (nextPage) {
    await ctx.publishStream<ISlackThreadStreamData>(
      `${SlackStreamType.THREADS}:${metadata.channelId}:${metadata.threadId}:${nextPage}`,
      {
        page: nextPage,
        channelId: metadata.channelId,
        token: metadata.token,
        channelsInfo: metadata.channelsInfo,
        teamUrl: metadata.teamUrl,
        team: metadata.team,
        channels: metadata.channels,
        threadId: metadata.threadId,
        channel: metadata.channel,
        new: metadata.new,
        placeholder: metadata.placeholder,
      },
    )
  }

  const messages = result.records as SlackMessage[]

  // publishing new streams for each thread
  while (messages.length > 0) {
    const message = messages.shift()
    const userId = message.user
    const member = await getSlackMember(userId, ctx)

    //  removing methions here instead of in the processData handler because we need to do API calls
    message.text = message.text ? await removeMentions(message.text, ctx) : ''

    // if member is undefined we don't publish the activity
    if (member !== undefined) {
      await ctx.processData<ISlackAPIData>({
        type: 'threads',
        message,
        member,
        channelId: metadata.channelId,
        threadId: metadata.threadId,
        channel: metadata.channel,
        placeholder: metadata.placeholder,
        base: {
          token: metadata.token,
          channelsInfo: metadata.channelsInfo,
          teamUrl: metadata.teamUrl,
          team: metadata.team,
          channels: metadata.channels,
        },
      })
    }
  }
}

const processMembersStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as ISlackMemberStreamData

  const result = await getMembers(
    {
      token: metadata.token,
      page: metadata.page,
      perPage: 200,
      teamId: metadata.team.id,
    },
    ctx,
  )

  const nextPage = result.nextPage

  // publishing next page stream
  if (nextPage) {
    await ctx.publishStream<ISlackMemberStreamData>(`${SlackStreamType.MEMBERS}:${nextPage}`, {
      page: nextPage,
      token: metadata.token,
      channelsInfo: metadata.channelsInfo,
      teamUrl: metadata.teamUrl,
      team: metadata.team,
      channels: metadata.channels,
    })
  }

  const members = result.records as SlackMember[]
  while (members.length > 0) {
    const member = members.shift()
    await ctx.processData<ISlackAPIData>({
      type: 'members',
      member,
      base: {
        token: metadata.token,
        channelsInfo: metadata.channelsInfo,
        teamUrl: metadata.teamUrl,
        team: metadata.team,
        channels: metadata.channels,
      },
    })
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  if (ctx.stream.identifier.startsWith(SlackStreamType.ROOT)) {
    await processRootStream(ctx)
  } else if (ctx.stream.identifier.startsWith(SlackStreamType.CHANNEL)) {
    await processChannelStream(ctx)
  } else if (ctx.stream.identifier.startsWith(SlackStreamType.MEMBERS)) {
    await processMembersStream(ctx)
  } else if (ctx.stream.identifier.startsWith(SlackStreamType.THREADS)) {
    await processThreadStream(ctx)
  } else {
    await ctx.abortRunWithError(`Unknown stream type: ${ctx.stream.identifier}`)
  }
}

export default handler
