import { IntegrationStreamType } from '@crowd/types'

import { IProcessStreamContext, ProcessStreamHandler } from '../../types'
import { getCommentComments } from './api/commentComments'
import { getMember } from './api/member'
import { getOrganization } from './api/organization'
import { getOrganizationPosts } from './api/organizationPosts'
import { getPostComments } from './api/postComments'
import { getPostReactions } from './api/postReactions'
import {
  ILinkedInAuthor,
  ILinkedInCachedMember,
  ILinkedInCachedOrganization,
  ILinkedInChildCommentCommentsStream,
  ILinkedInChildCommentData,
  ILinkedInChildPostCommentsStream,
  ILinkedInChildPostReactionsStream,
  ILinkedInCommentData,
  ILinkedInReactionData,
  ILinkedInRootOrganizationStream,
  LinkedinStreamType,
} from './types'
import {
  getLinkedInOrganizationId,
  getLinkedInUserId,
  isLinkedInOrganization,
  isLinkedInUser,
} from './utils'

const getLastReactionTs = async (
  ctx: IProcessStreamContext,
  postUrnId: string,
): Promise<number | undefined> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const posts = (ctx.integration.settings as any).posts || []
  const cachedPost = posts.find((p) => p.id === postUrnId)
  return cachedPost?.lastReactionTs
}

const getLastCommentTs = async (
  ctx: IProcessStreamContext,
  postUrnId: string,
): Promise<number | undefined> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const posts = (ctx.integration.settings as any).posts || []
  const cachedPost = posts.find((p) => p.id === postUrnId)
  return cachedPost?.lastCommentTs
}

const setLastReactionTs = async (ctx: IProcessStreamContext, postUrnId: string, ts: number) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const posts = (ctx.integration.settings as any).posts || []
  const cachedPost = posts.find((p) => p.id === postUrnId)
  if (cachedPost) {
    cachedPost.lastReactionTs = ts
  } else {
    posts.push({
      id: postUrnId,
      lastReactionTs: ts,
    })
  }

  await ctx.updateIntegrationSettings({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ...(ctx.integration.settings as any),
    posts,
  })
}

const setLastCommentTs = async (ctx: IProcessStreamContext, postUrnId: string, ts: number) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const posts = (ctx.integration.settings as any).posts || []
  const cachedPost = posts.find((p) => p.id === postUrnId)
  if (cachedPost) {
    cachedPost.lastCommentTs = ts
  } else {
    posts.push({
      id: postUrnId,
      lastCommentTs: ts,
    })
  }

  await ctx.updateIntegrationSettings({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ...(ctx.integration.settings as any),
    posts,
  })
}

const parseAuthor = async (
  memberUrn: string,
  ctx: IProcessStreamContext,
): Promise<ILinkedInAuthor> => {
  let user: ILinkedInAuthor

  if (isLinkedInUser(memberUrn)) {
    const userId = getLinkedInUserId(memberUrn)
    const userString = await ctx.cache.get(`user-${userId}`)

    if (userString) {
      user = {
        type: 'user',
        data: {
          ...JSON.parse(userString),
          userId,
        } as ILinkedInCachedMember,
      }
    } else {
      const data = await getMember(ctx.serviceSettings.nangoId, userId, ctx)
      user = {
        type: 'user',
        data: {
          ...data,
          userId,
        } as ILinkedInCachedMember,
      }
      await ctx.cache.set(`user-${userId}`, JSON.stringify(data), 7 * 24 * 60 * 60) // store for 7 days
    }
  } else if (isLinkedInOrganization(memberUrn)) {
    const userId = getLinkedInOrganizationId(memberUrn)
    const userString = await ctx.cache.get(`user-${userId}`)

    if (userString) {
      user = {
        type: 'organization',
        data: {
          ...JSON.parse(userString),
          userId,
        } as ILinkedInCachedOrganization,
      }
    } else {
      const data = await getOrganization(ctx.serviceSettings.nangoId, userId, ctx)
      user = {
        type: 'organization',
        data: {
          ...data,
          userId,
        } as ILinkedInCachedOrganization,
      }
      await ctx.cache.set(`user-${userId}`, JSON.stringify(data), 7 * 24 * 60 * 60) // store for 7 days
    }
  } else {
    await ctx.abortRunWithError(`Unknown member urn: ${memberUrn}`)
    throw new Error(`Unknown member urn: ${memberUrn}`)
  }

  return user
}

const processRootStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as ILinkedInRootOrganizationStream
  const organizationUrn = data.organizationUrn
  const organization = data.organization

  const results = await getOrganizationPosts(
    ctx.serviceSettings.nangoId,
    organizationUrn,
    ctx,
    data.start,
  )

  const posts = results.elements

  while (posts.length > 0) {
    const post = posts.shift()
    await ctx.cache.set(`post-${post.urnId}`, JSON.stringify(post), 2 * 24 * 60 * 60) // store for 2 days
    await ctx.publishStream<ILinkedInChildPostCommentsStream>(
      `${LinkedinStreamType.POST_COMMENTS}-${post.urnId}`,
      {
        postUrnId: post.urnId,
        postBody: post.body,
      },
    )
    await ctx.publishStream<ILinkedInChildPostReactionsStream>(
      `${LinkedinStreamType.POST_REACTIONS}-${post.urnId}`,
      {
        postUrnId: post.urnId,
        postBody: post.body,
      },
    )
  }

  if (results.start !== undefined) {
    await ctx.publishStream<ILinkedInRootOrganizationStream>(
      `${LinkedinStreamType.ORGANIZATION}-${organization}-${results.start}`,
      {
        organization,
        organizationUrn,
        start: results.start,
      },
    )
  }
}

const processPostReactionsStream: ProcessStreamHandler = async (ctx) => {
  const stream = ctx.stream.data as ILinkedInChildPostReactionsStream
  const postUrnId = stream.postUrnId
  const postBody = stream.postBody

  let lastReactionTs = await getLastReactionTs(ctx, postUrnId)
  if (!lastReactionTs && !ctx.onboarding) {
    const now = new Date()
    const oneMonthAgo = new Date(
      now.getFullYear(),
      now.getMonth() - 1,
      now.getDate(),
      now.getHours(),
      now.getMinutes(),
      now.getSeconds(),
      now.getMilliseconds(),
    )

    lastReactionTs = oneMonthAgo.valueOf()
  }

  const data = await getPostReactions(
    ctx.serviceSettings.nangoId,
    postUrnId,
    ctx,
    stream.start,
    lastReactionTs,
  )

  const reactions = data.elements

  while (reactions.length > 0) {
    const reaction = reactions.shift()

    if (lastReactionTs === undefined || lastReactionTs < reaction.timestamp) {
      await setLastReactionTs(ctx, postUrnId, reaction.timestamp)
    }

    const author = await parseAuthor(reaction.authorUrn, ctx)

    await ctx.processData<ILinkedInReactionData>({
      type: 'reaction',
      postUrnId,
      postBody,
      reaction,
      author,
    })
  }

  if (data.start !== undefined) {
    await ctx.publishStream<ILinkedInChildPostReactionsStream>(
      `${LinkedinStreamType.POST_REACTIONS}-${postUrnId}-${data.start}`,
      {
        postUrnId,
        postBody,
        start: data.start,
      },
    )
  }
}

const processPostCommentsStream: ProcessStreamHandler = async (ctx) => {
  const stream = ctx.stream.data as ILinkedInChildPostCommentsStream

  const postUrnId = stream.postUrnId
  const postBody = stream.postBody

  let lastCommentTs = await getLastCommentTs(ctx, postUrnId)
  if (!lastCommentTs && !ctx.onboarding) {
    const now = new Date()
    const oneMonthAgo = new Date(
      now.getFullYear(),
      now.getMonth() - 1,
      now.getDate(),
      now.getHours(),
      now.getMinutes(),
      now.getSeconds(),
      now.getMilliseconds(),
    )

    lastCommentTs = oneMonthAgo.valueOf()
  }

  const data = await getPostComments(
    ctx.serviceSettings.nangoId,
    postUrnId,
    ctx,
    stream.start,
    lastCommentTs,
  )

  const comments = data.elements

  while (comments.length > 0) {
    const comment = comments.shift()

    if (lastCommentTs === undefined || lastCommentTs < comment.timestamp) {
      await setLastCommentTs(ctx, postUrnId, comment.timestamp)
    }

    const author = await parseAuthor(comment.authorUrn, ctx)

    await ctx.processData<ILinkedInCommentData>({
      type: 'comment',
      comment,
      postUrnId,
      postBody,
      author,
    })
  }

  if (data.start !== undefined) {
    await ctx.publishStream<ILinkedInChildPostCommentsStream>(
      `${LinkedinStreamType.POST_COMMENTS}-${postUrnId}-${data.start}`,
      {
        postUrnId,
        postBody,
        start: data.start,
      },
    )
  }
}

const processCommentCommentsStream: ProcessStreamHandler = async (ctx) => {
  const stream = ctx.stream.data as ILinkedInChildCommentCommentsStream

  const data = await getCommentComments(
    ctx.serviceSettings.nangoId,
    stream.commentUrnId,
    ctx,
    stream.start,
  )

  const comments = data.elements

  while (comments.length > 0) {
    const comment = comments.shift()

    const author = await parseAuthor(comment.authorUrn, ctx)

    await ctx.processData<ILinkedInChildCommentData>({
      type: 'child_comment',
      parentCommentUrnId: stream.commentUrnId,
      comment,
      postUrnId: stream.postUrnId,
      postBody: stream.postBody,
      author,
    })
  }

  if (data.start !== undefined) {
    await ctx.publishStream<ILinkedInChildCommentCommentsStream>(
      `${LinkedinStreamType.COMMENT_COMMENTS}-${stream.commentUrnId}-${data.start}`,
      {
        postUrnId: stream.postUrnId,
        postBody: stream.postBody,
        commentUrnId: stream.commentUrnId,
        start: data.start,
      },
    )
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  if (
    ctx.stream.type === IntegrationStreamType.ROOT ||
    ctx.stream.identifier.startsWith(LinkedinStreamType.ORGANIZATION)
  ) {
    await processRootStream(ctx)
  } else {
    if (ctx.stream.identifier.startsWith(LinkedinStreamType.POST_COMMENTS)) {
      await processPostCommentsStream(ctx)
    } else if (ctx.stream.identifier.startsWith(LinkedinStreamType.POST_REACTIONS)) {
      await processPostReactionsStream(ctx)
    } else if (ctx.stream.identifier.startsWith(LinkedinStreamType.COMMENT_COMMENTS)) {
      await processCommentCommentsStream(ctx)
    } else {
      {
        ctx.abortRunWithError(`Unknown child stream identifier: ${ctx.stream.identifier}`)
      }
    }
  }
}

export default handler
