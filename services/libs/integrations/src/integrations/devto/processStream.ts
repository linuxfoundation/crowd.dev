import { IntegrationStreamType } from '@crowd/types'

import { IProcessStreamContext, ProcessStreamHandler } from '../../types'
import { IDevToArticle, getArticle, getOrganizationArticles, getUserArticles } from './api/articles'
import { getArticleComments } from './api/comments'
import { IDevToUser, getUser } from './api/user'
import {
  DevToRootStream,
  IDevToArticleData,
  IDevToRootOrganizationStreamData,
  IDevToRootUserStreamData,
} from './types'
import { getUserIdsFromComments, setFullUser } from './utils'

const getDevApiKey = (ctx: IProcessStreamContext) => {
  return ctx.integration.token
}

const getDevToArticle = async (ctx: IProcessStreamContext, id: number): Promise<IDevToArticle> => {
  const cached = await ctx.cache.get(`article:${id}`)
  if (cached) {
    return JSON.parse(cached)
  }

  const article = await getArticle(id, getDevApiKey(ctx))
  await ctx.cache.set(`article:${id}`, JSON.stringify(article), 7 * 24 * 60 * 60) // store for 7 days
  return article
}

const getDevToUser = async (ctx: IProcessStreamContext, userId: number): Promise<IDevToUser> => {
  const cached = await ctx.cache.get(`user:${userId}`)
  if (cached) {
    return JSON.parse(cached)
  }

  const user = await getUser(userId, getDevApiKey(ctx))
  await ctx.cache.set(`user:${userId}`, JSON.stringify(user), 7 * 24 * 60 * 60) // store for 7 days
  return user
}

const processRootStream: ProcessStreamHandler = async (ctx) => {
  // no data here just new streams
  if (ctx.stream.identifier.startsWith(DevToRootStream.ORGANIZATION_ARTICLES)) {
    const organization = (ctx.stream.data as IDevToRootOrganizationStreamData).organization
    let page = 1
    let articles = await getOrganizationArticles(organization, page, 20, getDevApiKey(ctx))
    while (articles.length > 0) {
      for (const article of articles) {
        ctx.log.debug(`Creating organization article stream with identifier ${article.id}!`)
        await ctx.cache.set(`article:${article.id}`, JSON.stringify(article), 7 * 24 * 60 * 60) // store for 7 days
        await ctx.publishStream(`${article.id}`)
      }
      articles = await getOrganizationArticles(organization, ++page, 20, getDevApiKey(ctx))
    }
  } else if (ctx.stream.identifier.startsWith(DevToRootStream.USER_ARTICLES)) {
    const user = (ctx.stream.data as IDevToRootUserStreamData).user
    let page = 1
    let articles = await getUserArticles(user, page, 20, getDevApiKey(ctx))
    while (articles.length > 0) {
      for (const article of articles) {
        ctx.log.debug(`Creating user article stream with identifier ${article.id}!`)
        await ctx.cache.set(`article:${article.id}`, JSON.stringify(article), 7 * 24 * 60 * 60) // store for 7 days
        await ctx.publishStream(`${article.id}`)
      }
      articles = await getUserArticles(user, ++page, 20, getDevApiKey(ctx))
    }
  } else {
    await ctx.abortWithError(`Unknown root stream identifier: ${ctx.stream.identifier}`)
  }
}

const processArticleStream: ProcessStreamHandler = async (ctx) => {
  const articleId = parseInt(ctx.stream.identifier, 10)

  ctx.log.debug({ devtoArticleId: articleId }, 'Processing article stream!')

  const comments = await getArticleComments(articleId, getDevApiKey(ctx))

  if (comments.length > 0) {
    ctx.log.debug(
      { devtoArticleId: articleId, nComments: comments.length },
      'We have found comments for this article!',
    )
    const userIds = getUserIdsFromComments(comments)
    for (const userId of userIds) {
      const fullUser = await getDevToUser(ctx, userId)
      if (fullUser) {
        setFullUser(comments, fullUser)
      }
    }

    await ctx.processData<IDevToArticleData>({
      article: await getDevToArticle(ctx, articleId),
      comments,
    })
  } else {
    ctx.log.debug({ devtoArticleId: articleId }, 'No comments found for this article!')
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  if (ctx.stream.type === IntegrationStreamType.ROOT) {
    await processRootStream(ctx)
  } else {
    await processArticleStream(ctx)
  }
}

export default handler
