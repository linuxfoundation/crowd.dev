// processStream.ts content
import { generateUUIDv4 } from '@crowd/common'

import { ProcessStreamHandler } from '../../types'
import { getDiscourseCategories } from './api/getCategories'
import { getDiscoursePostsByIds } from './api/getPostsByIds'
import { getDiscoursePostsFromTopic } from './api/getPostsFromTopic'
import { getDiscourseTopics } from './api/getTopics'
import { getDiscourseUserByUsername } from './api/getUser'
import {
  DiscourseConnectionParams,
  DiscourseDataType,
  DiscoursePostsByIdsInput,
  DiscoursePostsByIdsStreamData,
  DiscoursePostsFromTopicStreamData,
  DiscoursePostsInput,
  DiscoursePublishPostData,
  DiscourseStreamType,
  DiscourseTopicsFromCategoryStreamData,
  DiscourseTopicsInput,
} from './types'

const BOT_USERNAMES = ['system', 'discobot']

export const usernameIsBot = (username: string): boolean => BOT_USERNAMES.includes(username)

const processCategoriesStream: ProcessStreamHandler = async (ctx) => {
  const { forumHostname, apiKey, apiUsername } = ctx.integration
    .settings as DiscourseConnectionParams

  const params: DiscourseConnectionParams = {
    forumHostname,
    apiKey,
    apiUsername,
  }
  const discourseCategories = await getDiscourseCategories(params, ctx)

  // publishing new streams to get topics from each category
  discourseCategories.category_list.categories.forEach(async (category) => {
    await ctx.publishStream<DiscourseTopicsFromCategoryStreamData>(
      `${DiscourseStreamType.TOPICS_FROM_CATEGORY}:${category.id}`,
      {
        category_id: category.id,
        category_slug: category.slug,
        page: 0,
      },
    )
  })
}

const processTopicsFromCategoryStream: ProcessStreamHandler = async (ctx) => {
  const { forumHostname, apiKey, apiUsername } = ctx.integration
    .settings as DiscourseConnectionParams

  const metadata = ctx.stream.data as DiscourseTopicsFromCategoryStreamData

  const params: DiscourseConnectionParams = {
    forumHostname,
    apiKey,
    apiUsername,
  }

  const input: DiscourseTopicsInput = {
    category_id: metadata.category_id,
    category_slug: metadata.category_slug,
    page: metadata.page,
  }

  const discourseTopics = await getDiscourseTopics(params, input, ctx)

  if (discourseTopics?.topic_list?.topics?.length > 0) {
    discourseTopics.topic_list.topics.forEach(async (topic) => {
      await ctx.publishStream<DiscoursePostsFromTopicStreamData>(
        `${DiscourseStreamType.POSTS_FROM_TOPIC}:${topic.id}`,
        {
          topicId: topic.id,
          topic_slug: topic.slug,
          page: 0,
        },
      )
    })

    // we aslo need to publish new stream to get next page of topics
    await ctx.publishStream<DiscourseTopicsFromCategoryStreamData>(
      `${DiscourseStreamType.TOPICS_FROM_CATEGORY}:${metadata.category_id}`,
      {
        category_id: metadata.category_id,
        category_slug: metadata.category_slug,
        page: metadata.page + 1,
      },
    )
  }
}

const processPostsFromTopicStream: ProcessStreamHandler = async (ctx) => {
  const { forumHostname, apiKey, apiUsername } = ctx.integration
    .settings as DiscourseConnectionParams

  const metadata = ctx.stream.data as DiscoursePostsFromTopicStreamData

  const params: DiscourseConnectionParams = {
    forumHostname,
    apiKey,
    apiUsername,
  }

  const input: DiscoursePostsInput = {
    topic_slug: metadata.topic_slug,
    topic_id: metadata.topicId,
    page: metadata.page,
  }

  const discoursePosts = await getDiscoursePostsFromTopic(params, input, ctx)

  const batchSize = 30
  const postBatches: number[][] = []

  discoursePosts?.post_stream?.stream?.forEach((postId, index) => {
    if (index % batchSize === 0) {
      postBatches.push([])
    }
    postBatches[postBatches.length - 1].push(postId)
  })

  postBatches.forEach(async (postBatch, index) => {
    await ctx.publishStream<DiscoursePostsByIdsStreamData>(
      `${DiscourseStreamType.POSTS_BY_IDS}:${generateUUIDv4()}`,
      {
        topicId: discoursePosts.id,
        topicSlug: discoursePosts.slug,
        topicTitle: discoursePosts.title,
        postIds: postBatch,
        lastIdInPreviousBatch:
          index === 0 ? undefined : postBatches[index - 1][postBatches[index - 1].length - 1],
      },
    )
  })
}

const processPostsByIds: ProcessStreamHandler = async (ctx) => {
  const { forumHostname, apiKey, apiUsername } = ctx.integration
    .settings as DiscourseConnectionParams

  const metadata = ctx.stream.data as DiscoursePostsByIdsStreamData

  const params: DiscourseConnectionParams = {
    forumHostname,
    apiKey,
    apiUsername,
  }

  const input: DiscoursePostsByIdsInput = {
    topic_id: metadata.topicId,
    post_ids: metadata.postIds,
  }

  const discoursePosts = await getDiscoursePostsByIds(params, input, ctx)

  const posts = discoursePosts?.post_stream?.posts

  for (const post of posts) {
    if (usernameIsBot(post.username)) {
      /* eslint-disable no-continue */
      continue
    }
    const user = await getDiscourseUserByUsername(
      {
        forumHostname,
        apiKey,
        apiUsername,
      },
      { username: post.username },
      ctx,
    )

    if (!user) {
      continue
    }

    await ctx.processData<DiscoursePublishPostData>({
      type: DiscourseDataType.POST,
      user,
      post,
      topicId: metadata.topicId,
      topicSlug: metadata.topicSlug,
      topicTitle: metadata.topicTitle,
      lastIdInPreviousBatch: metadata.lastIdInPreviousBatch,
    })
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  const streamType = ctx.stream.identifier.split(':')[0]
  switch (streamType) {
    case DiscourseStreamType.CATEGORIES:
      await processCategoriesStream(ctx)
      break
    case DiscourseStreamType.TOPICS_FROM_CATEGORY:
      await processTopicsFromCategoryStream(ctx)
      break
    case DiscourseStreamType.POSTS_FROM_TOPIC:
      await processPostsFromTopicStream(ctx)
      break
    case DiscourseStreamType.POSTS_BY_IDS:
      await processPostsByIds(ctx)
      break
    default:
      throw new Error(`Unknown stream type: ${streamType}`)
  }
}

export default handler
