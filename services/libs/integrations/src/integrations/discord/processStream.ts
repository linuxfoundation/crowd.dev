import axios from 'axios'

import { timeout } from '@crowd/common'
import { RateLimitError } from '@crowd/types'

import { IProcessStreamContext, ProcessStreamHandler } from '../../types'
import { getChannel } from './api/getChannel'
import getChannels from './api/getChannels'
import getMembers from './api/getMembers'
import getMessages from './api/getMessages'
import getThreads from './api/getThreads'
import { ChannelType } from './externalTypes'
import {
  DiscordAPIDataType,
  DiscordApiChannel,
  DiscordApiDataMessage,
  DiscordApiMessage,
  DiscordChannelStreamData,
  DiscordMemberStreamData,
  DiscordRootStreamData,
  DiscordStreamType,
  IDiscordAPIData,
  IDiscordIntegrationSettings,
  IDiscordPlatformSettings,
} from './types'

const MAX_RETROSPECT_SECONDS = 86400 // 24 hours

async function saveLastTimestampForChannel(
  streamIdentifier: string,
  timestamp: string,
  ctx: IProcessStreamContext,
): Promise<void> {
  const cacheKey = `discord:channels:lastTimestamp`

  const prefix = (key: string): string => `${cacheKey}:${key}`
  await ctx.cache.set(prefix(streamIdentifier), timestamp, 24 * 60 * 60)
}

async function getLastTimestampForChannel(
  streamIdentifier: string,
  ctx: IProcessStreamContext,
): Promise<string | undefined> {
  const cacheKey = `discord:channels:lastTimestamp`

  const prefix = (key: string): string => `${cacheKey}:${key}`
  const cached = await ctx.cache.get(prefix(streamIdentifier))

  return cached
}

export function isDiscordForum(channel: DiscordApiChannel): boolean {
  return channel.type === ChannelType.GuildForum
}

export function isDiscordThread(channel: DiscordApiChannel): boolean {
  return channel.type === ChannelType.PublicThread || channel.type === ChannelType.PrivateThread
}

async function getDiscordChannel(
  id: string,
  ctx: IProcessStreamContext,
): Promise<DiscordApiChannel> {
  const cache = ctx.cache
  const cacheKey = `discord:channels`

  const prefix = (key: string): string => `${cacheKey}:${key}`

  const cached = await cache.get(prefix(id))

  if (cached) {
    return JSON.parse(cached)
  }

  const channel = await getChannel(id, getDiscordToken(ctx), ctx)
  await cache.set(id, JSON.stringify(channel), 24 * 60 * 60)

  return channel
}

export const cacheDiscordChannels = async (
  channel: DiscordApiChannel,
  ctx: IProcessStreamContext,
): Promise<void> => {
  const cacheKey = `discord:channels`

  const prefix = (key: string): string => `${cacheKey}:${key}`

  await ctx.cache.set(prefix(channel.id), JSON.stringify(channel), 24 * 60 * 60)
}

export const getDiscordToken = (ctx: IProcessStreamContext): string => {
  if (ctx.integration.token) {
    return `Bot ${ctx.integration.token}`
  }
  const token = ctx.platformSettings as IDiscordPlatformSettings
  return `Bot ${token.token}`
}

const processRootStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as DiscordRootStreamData
  const guildId = data.guildId

  const allowedChannels: DiscordApiChannel[] = []
  const forbiddenChannels: DiscordApiChannel[] = []

  const fromDiscordApi = await getChannels(
    {
      guildId,
      token: getDiscordToken(ctx),
    },
    ctx,
  )

  if (fromDiscordApi.length === 0) {
    ctx.log.warn(`No avaliable channels found for guild ${guildId}, skipping...`)
    return
  }

  for (const channel of fromDiscordApi) {
    try {
      const config = {
        method: 'get',
        url: `https://discord.com/api/v10/channels/${channel.id}`,
        headers: {
          Authorization: getDiscordToken(ctx),
        },
      }

      await axios(config)
      allowedChannels.push(channel)
    } catch (err) {
      if (err.response && err.response.status === 403) {
        forbiddenChannels.push(channel)
      } else {
        throw err
      }
    }
  }

  for (const channel of allowedChannels) {
    await cacheDiscordChannels(channel, ctx)
  }

  const threads = await getThreads(
    {
      guildId,
      token: getDiscordToken(ctx),
    },
    ctx,
  )

  // we are only interested in threads that are in a forum channel because the rest we will get normally when a message has thread property attached
  for (const thread of threads) {
    // check if thread.parent_id is in allowedChannels
    if (thread.parent_id && allowedChannels.find((c) => c.id === thread.parent_id)) {
      await cacheDiscordChannels(thread, ctx)
      const parentChannel = await getDiscordChannel(thread.parent_id, ctx)
      if (isDiscordForum(parentChannel)) {
        allowedChannels.push(thread)
      }
    } else {
      forbiddenChannels.push(thread)
    }
  }

  // publishing stream to get members
  await ctx.publishStream<DiscordMemberStreamData>(DiscordStreamType.MEMBERS, {
    page: '',
    guildId,
    channels: allowedChannels,
  })

  for (const channel of allowedChannels) {
    await ctx.publishStream<DiscordChannelStreamData>(
      `${DiscordStreamType.CHANNEL}:${channel.id}`,
      {
        channelId: channel.id,
        guildId,
        page: '',
        channels: allowedChannels,
      },
    )
  }

  // updating settings
  const currentSettings = ctx.integration.settings as IDiscordIntegrationSettings
  const newSettings = {
    ...currentSettings,
    channels: allowedChannels.map((c) => ({
      id: c.id,
      name: c.name,
    })),
    forbiddenChannels: forbiddenChannels.map((c) => ({
      id: c.id,
      name: c.name,
    })),
  }

  await ctx.updateIntegrationSettings(newSettings)
}

const processMembersStream: ProcessStreamHandler = async (ctx) => {
  await timeout(2000)
  const data = ctx.stream.data as DiscordMemberStreamData

  const members = await getMembers(
    {
      guildId: data.guildId,
      token: getDiscordToken(ctx),
      page: data.page,
      perPage: 1000,
    },
    ctx,
  )

  if (members.nextPage) {
    await ctx.publishStream<DiscordMemberStreamData>(
      `${DiscordStreamType.MEMBERS}:${members.nextPage}`,
      {
        page: members.nextPage,
        guildId: data.guildId,
        channels: data.channels,
      },
    )
  }

  const sleep = members.limit <= 1 ? members.timeUntilReset : undefined

  if (members.records.length > 0) {
    await ctx.processData<IDiscordAPIData>({
      type: DiscordAPIDataType.MEMBER,
      data: members.records,
    })
  }

  if (sleep) {
    throw new RateLimitError(sleep, 'getMembers')
  }
}

const processChannelStream: ProcessStreamHandler = async (ctx) => {
  await timeout(2000)
  const data = ctx.stream.data as DiscordChannelStreamData

  const channelMessages = await getMessages(
    {
      channelId: data.channelId,
      token: getDiscordToken(ctx),
      page: data.page,
      perPage: 100,
    },
    ctx,
  )

  // if we are not in onboarding mode we need to check if we are not going too far in the past
  if (!ctx.onboarding) {
    // checking if we are not going too far in the past
    const lastTimestamp = await getLastTimestampForChannel(ctx.stream.identifier, ctx)
    if (lastTimestamp) {
      const lastTimestampDate = new Date(lastTimestamp)
      const lastTimestampSeconds = lastTimestampDate.getTime() / 1000
      const nowSeconds = Date.now() / 1000
      const diff = nowSeconds - lastTimestampSeconds
      if (diff > MAX_RETROSPECT_SECONDS) {
        ctx.log.warn(
          `Going too far in the past for channel ${
            ctx.stream.identifier
          }. Last timestamp is ${lastTimestampDate.toISOString()}`,
        )
        // we don't need to parse next page, still need to parse threads
        channelMessages.nextPage = ''
      }
    }
  }

  if (channelMessages.nextPage) {
    await ctx.publishStream<DiscordChannelStreamData>(
      `${DiscordStreamType.CHANNEL}:${data.channelId}:${channelMessages.nextPage}`,
      {
        channelId: data.channelId,
        guildId: data.guildId,
        page: channelMessages.nextPage,
        channels: data.channels,
      },
    )
  }

  for (const record of channelMessages.records as DiscordApiMessage[]) {
    let parent: string | undefined
    let parentChannel: string | undefined

    let firstThreadMessage = false
    // is the message starting a thread? if so we should get thread messages as well
    if (record.thread) {
      await ctx.publishStream<DiscordChannelStreamData>(
        `${DiscordStreamType.CHANNEL}:${record.thread.id}`,
        {
          channelId: record.thread.id,
          guildId: data.guildId,
          page: '',
          channels: data.channels,
        },
      )

      firstThreadMessage = true
      await cacheDiscordChannels(record.thread as DiscordApiChannel, ctx)
    }

    if (record.id === record.channel_id) {
      firstThreadMessage = true
    }

    const channel = await getChannel(record.channel_id, getDiscordToken(ctx), ctx)

    let isForum = false
    const isThread = isDiscordThread(channel)

    if (isThread || isForum) {
      if (!firstThreadMessage) {
        parent = channel.id
      }

      if (channel.parent_id) {
        const parentChannelObj = await getChannel(channel.parent_id, getDiscordToken(ctx), ctx)
        parentChannel = parentChannelObj.name
        isForum = isDiscordForum(parentChannelObj)
      }
    }
    // record.parentId means that it's a reply
    else if (record.message_reference && record.message_reference.message_id) {
      parent = record.message_reference.message_id
    }

    await ctx.processData<IDiscordAPIData>({
      type: DiscordAPIDataType.CHANNEL,
      data: {
        ...record,
        parent,
        parentChannel,
        isForum,
        isThread,
        channel,
      } as DiscordApiDataMessage,
    })
  }

  // saving last timestamp from the stream
  const records = channelMessages.records as DiscordApiMessage[]
  if (records.length > 0) {
    const lastRecord = records[records.length - 1]
    const lastRecordDate = lastRecord.timestamp
    await saveLastTimestampForChannel(ctx.stream.identifier, lastRecordDate, ctx)
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  const streamIdentifier = ctx.stream.identifier

  if (streamIdentifier.startsWith(DiscordStreamType.ROOT)) {
    await processRootStream(ctx)
  } else if (streamIdentifier.startsWith(DiscordStreamType.MEMBERS)) {
    await processMembersStream(ctx)
  } else if (streamIdentifier.startsWith(DiscordStreamType.CHANNEL)) {
    await processChannelStream(ctx)
  } else {
    await ctx.abortRunWithError(`Unknown stream type: ${streamIdentifier}`)
  }
}

export default handler
