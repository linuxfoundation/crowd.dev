import { ProcessStreamHandler } from '../../types'
import getPost from './api/getPost'
import getPostsByKeywords from './api/getPostsByKeywords'
import {
  HackerNewsInitialStreamMetadata,
  HackerNewsMainStreamMetadata,
  HackerNewsPublishData,
  HackerNewsStreamType,
} from './types'

const processInitialStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as HackerNewsInitialStreamMetadata

  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
  const unixTimestamp = Math.floor(thirtyDaysAgo.getTime() / 1000)
  const posts = await getPostsByKeywords(
    {
      keywords: metadata.keywords,
      after: ctx.onboarding ? 0 : unixTimestamp,
    },
    ctx,
  )

  while (posts.length > 0) {
    const post = posts.shift()
    await ctx.publishStream<HackerNewsMainStreamMetadata>(
      `${HackerNewsStreamType.MAIN}:${post.postId}`,
      {
        postId: post.postId,
        channel: post.keywords[0],
      },
    )
  }
}

const processMainStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as HackerNewsMainStreamMetadata

  const post = await getPost(metadata.postId.toString(), ctx)

  if (post.kids !== undefined) {
    for (const kid of post.kids) {
      await ctx.publishStream<HackerNewsMainStreamMetadata>(`${HackerNewsStreamType.MAIN}:${kid}`, {
        postId: kid,
        channel: metadata.channel,
        ...(!post.parent && {
          parentId: post.id.toString(),
          parentTitle: post.title || post.text,
        }),
      })
    }
  }

  if (post.text || post.url) {
    await ctx.processData<HackerNewsPublishData>({
      post,
      channel: metadata.channel,
      parentId: metadata.parentId,
      parentTitle: metadata.parentTitle,
    })
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  if (ctx.stream.identifier.startsWith(HackerNewsStreamType.INITIAL)) {
    await processInitialStream(ctx)
  } else if (ctx.stream.identifier.startsWith(HackerNewsStreamType.MAIN)) {
    await processMainStream(ctx)
  } else {
    await ctx.abortRunWithError(`Unknown stream type: ${ctx.stream.identifier}`)
  }
}

export default handler
