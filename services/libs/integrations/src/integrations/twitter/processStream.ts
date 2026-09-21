import { processPaginated } from '@crowd/common'
import { generateUUIDv4 } from '@crowd/common'
import { fetchIntegrationMembersPaginated } from '@crowd/data-access-layer/src/old/lib/integrations/members'
import { MemberIdentityType, PlatformType, RateLimitError } from '@crowd/types'

import { ProcessStreamHandler } from '../../types'
import getPostsByHashtag from './api/getPostsByHashtag'
import getPostsByMention from './api/getPostsByMention'
import getProfiles from './api/getProfiles'
import {
  TwitterHashtagStreamData,
  TwitterMentionsStreamData,
  TwitterPublishData,
  TwitterReachStreamData,
  TwitterStreamType,
} from './types'

const processMentionsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as TwitterMentionsStreamData

  const { records, nextPage, limit, timeUntilReset } = await getPostsByMention(
    {
      token: ctx.integration.token,
      page: data.page,
      perPage: 100,
      profileId: ctx.integration.identifier,
    },
    ctx,
  )

  const sleep = limit <= 1 ? timeUntilReset : undefined

  if (sleep) {
    throw new RateLimitError(sleep, 'twitter/mentions')
  }

  if (records && records.length && records.length > 0) {
    for (const rec of records) {
      await ctx.processData<TwitterPublishData>({
        type: TwitterStreamType.MENTIONS,
        data: rec,
      })
    }
  }

  if (nextPage) {
    await ctx.publishStream<TwitterMentionsStreamData>(
      `${TwitterStreamType.MENTIONS}:${nextPage}`,
      {
        page: nextPage,
      },
    )
  }
}

const processHashtagStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as TwitterHashtagStreamData

  const { records, nextPage, limit, timeUntilReset } = await getPostsByHashtag(
    {
      token: ctx.integration.token,
      page: data.page,
      perPage: 100,
      hashtag: data.hashtag,
    },
    ctx,
  )

  const sleep = limit <= 1 ? timeUntilReset : undefined

  if (sleep) {
    throw new RateLimitError(sleep, 'twitter/hashtag')
  }

  if (records && records.length && records.length > 0) {
    for (const rec of records) {
      await ctx.processData<TwitterPublishData>({
        type: TwitterStreamType.HASHTAG,
        data: rec,
        hashtag: data.hashtag,
      })
    }
  }

  if (nextPage) {
    await ctx.publishStream<TwitterHashtagStreamData>(`${TwitterStreamType.HASHTAG}:${nextPage}`, {
      page: nextPage,
      hashtag: data.hashtag,
    })
  }
}

const processReachStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as TwitterReachStreamData
  const usernames = data.usernames

  if (!usernames) {
    // this is the initial stream, we need to get all the usernames
    const perPage = 100
    const db = ctx.getDbConnection()

    ctx.log.info('Getting all usernames for reach update', { int: ctx.integration })

    await processPaginated(
      async (page) => {
        return await fetchIntegrationMembersPaginated(
          db,
          ctx.integration.id,
          PlatformType.TWITTER,
          MemberIdentityType.USERNAME,
          page,
          perPage,
        )
      },
      async (members) => {
        const usernames = members.map((m) => m.value)
        await ctx.publishStream<TwitterReachStreamData>(
          `${TwitterStreamType.REACH}:${generateUUIDv4()}`,
          {
            usernames,
          },
        )
      },
    )
  } else {
    // this is a stream for a specific set of usernames
    const { records, limit, timeUntilReset } = await getProfiles(
      {
        usernames,
        token: ctx.integration.token,
      },
      ctx,
    )

    const sleep = limit <= 1 ? timeUntilReset : undefined

    if (sleep) {
      throw new RateLimitError(sleep, 'twitter/reach')
    }

    if (records && records.length && records.length > 0) {
      for (const rec of records) {
        await ctx.processData<TwitterPublishData>({
          type: TwitterStreamType.REACH,
          member: rec,
        })
      }
    }
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  if (!ctx.stream.identifier) {
    throw new Error('Stream identifier is required')
  }
  const streamIdentifier = ctx.stream.identifier.split(':')

  switch (streamIdentifier[0]) {
    case TwitterStreamType.MENTIONS:
      await processMentionsStream(ctx)
      break
    case TwitterStreamType.HASHTAG:
      await processHashtagStream(ctx)
      break
    case TwitterStreamType.REACH:
      await processReachStream(ctx)
      break
    default:
      throw new Error(`Unknown stream identifier: ${ctx.stream.identifier}`)
  }
}

export default handler
