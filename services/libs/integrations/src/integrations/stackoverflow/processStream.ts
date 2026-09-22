import { generateUUIDv4 } from '@crowd/common'

import { IProcessStreamContext, ProcessStreamHandler } from '../../types'
import getAnswers from './api/getAnswers'
import getQuestionsByTags from './api/getQuestions'
import getQuestionsByKeyword from './api/getQuestionsByKeywords'
import getUser from './api/getUser'
import {
  IStackOverflowAnswerStreamData,
  IStackOverflowKeywordStreamData,
  IStackOverflowPublishData,
  IStackOverflowTagStreamData,
  StackOverflowAnswerResponse,
  StackOverflowQuestionsResponse,
  StackOverflowRootStream,
  StackOverflowUser,
} from './types'

const getStackOverflowUser = async (
  ctx: IProcessStreamContext,
  userId: string,
): Promise<StackOverflowUser> => {
  const cached = await ctx.cache.get(`user:${userId}`)
  if (cached) {
    return JSON.parse(cached)
  }

  const user = await getUser({ userId, nangoId: ctx.serviceSettings.nangoId }, ctx)
  await ctx.cache.set(`user:${userId}`, JSON.stringify(user), 7 * 24 * 60 * 60) // store for 7 days
  return user
}

const processTagStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as IStackOverflowTagStreamData
  const tags = metadata.tags
  const page = metadata.page || 1
  const response: StackOverflowQuestionsResponse = await getQuestionsByTags(
    { tags, page, nangoId: ctx.serviceSettings.nangoId },
    ctx,
  )

  const questions = response.items

  if (questions.length === 0) {
    return
  }

  // Shows if there are more pages to parse
  const hasMore = response.has_more

  // If there are more pages, we need to create a new stream to get the next page
  if (questions.length > 0 && hasMore) {
    await ctx.publishStream<IStackOverflowTagStreamData>(
      `${StackOverflowRootStream.QUESTIONS_BY_TAG}:${tags[0]}:${page + 1}`,
      {
        tags: tags,
        page: page + 1,
      },
    )
  }

  // pulblish all questions and new streams for answers
  while (questions.length > 0) {
    const question = questions.shift()
    let user: StackOverflowUser = null
    if (!question.owner) {
      // we completely ignore questions without owner - they are not valid
      continue
    } else if (!question.owner.user_id) {
      // this is deleted user or similar
      user = {
        user_id: generateUUIDv4(),
        display_name: question.owner?.display_name || 'Deleted User',
      } as StackOverflowUser
    } else {
      user = await getStackOverflowUser(ctx, question.owner.user_id.toString())
    }
    await ctx.processData<IStackOverflowPublishData>({
      question: {
        question,
        user,
        tag: tags[0],
        keyword: null,
      },
      answer: null,
    })

    await ctx.publishStream<IStackOverflowAnswerStreamData>(
      `${StackOverflowRootStream.ANSWERS_TO_QUESTION}:${question.question_id}:${1}`,
      {
        questionId: question.question_id.toString(),
        tag: tags[0],
        keyword: null,
        page: 1,
      },
    )
  }
}

const processKeywordStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as IStackOverflowKeywordStreamData
  const keyword = metadata.keyword
  const page = metadata.page || 1

  const response: StackOverflowQuestionsResponse = await getQuestionsByKeyword(
    { keyword, page, nangoId: ctx.serviceSettings.nangoId },
    ctx,
  )

  const questions = response.items

  if (questions.length === 0) {
    return
  }

  // Shows if there are more pages to parse
  const hasMore = response.has_more

  // If there are more pages, we need to create a new stream to get the next page
  if (questions.length > 0 && hasMore) {
    await ctx.publishStream<IStackOverflowKeywordStreamData>(
      `${StackOverflowRootStream.QUESTIONS_BY_KEYWORD}:${keyword}:${page + 1}`,
      {
        keyword,
        page: page + 1,
      },
    )
  }

  // pulblish all questions and new streams for answers
  while (questions.length > 0) {
    const question = questions.shift()
    let user: StackOverflowUser = null
    if (!question.owner) {
      // we completely ignore questions without owner - they are not valid
      continue
    } else if (!question.owner.user_id) {
      // this is deleted user or similar
      user = {
        user_id: generateUUIDv4(),
        display_name: question.owner?.display_name || 'Deleted User',
      } as StackOverflowUser
    } else {
      user = await getStackOverflowUser(ctx, question.owner.user_id.toString())
    }
    await ctx.processData<IStackOverflowPublishData>({
      question: {
        question,
        user,
        tag: null,
        keyword,
      },
      answer: null,
    })

    await ctx.publishStream<IStackOverflowAnswerStreamData>(
      `${StackOverflowRootStream.ANSWERS_TO_QUESTION}:${question.question_id}:${1}`,
      {
        questionId: question.question_id.toString(),
        tag: null,
        keyword,
        page: 1,
      },
    )
  }
}

const processAnswerStream: ProcessStreamHandler = async (ctx) => {
  const metadata = ctx.stream.data as IStackOverflowAnswerStreamData
  const questionId = metadata.questionId
  const page = metadata.page || 1
  const tag = metadata.tag
  const keyword = metadata.keyword
  const response: StackOverflowAnswerResponse = await getAnswers(
    { questionId, page, nangoId: ctx.serviceSettings.nangoId },
    ctx,
  )

  const answers = response.items

  if (answers.length === 0) {
    return
  }
  // Shows if there are more pages to parse
  const hasMore = response.has_more

  // If there are more pages, we need to create a new stream to get the next page
  if (answers.length > 0 && hasMore) {
    await ctx.publishStream<IStackOverflowAnswerStreamData>(
      `${StackOverflowRootStream.ANSWERS_TO_QUESTION}:${questionId}:${page + 1}`,
      {
        questionId,
        tag,
        keyword,
        page: page + 1,
      },
    )
  }

  // pulblish all questions and new streams for answers
  let previousAnswerId: string | null = null
  while (answers.length > 0) {
    const answer = answers.shift()
    let user: StackOverflowUser = null
    if (!answer.owner) {
      // we completely ignore answers without owner - they are not valid
      continue
    } else if (!answer.owner.user_id) {
      // this is deleted user or similar
      user = {
        user_id: generateUUIDv4(),
        display_name: answer.owner?.display_name || 'Deleted User',
      } as StackOverflowUser
    } else {
      user = await getStackOverflowUser(ctx, answer.owner.user_id.toString())
    }
    await ctx.processData<IStackOverflowPublishData>({
      answer: {
        answer,
        user,
        tag,
        keyword,
        previousAnswerId,
      },
      question: null,
    })

    previousAnswerId = answer.answer_id.toString()
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  if (ctx.stream.identifier.startsWith(StackOverflowRootStream.QUESTIONS_BY_TAG)) {
    await processTagStream(ctx)
  } else if (ctx.stream.identifier.startsWith(StackOverflowRootStream.QUESTIONS_BY_KEYWORD)) {
    await processKeywordStream(ctx)
  } else if (ctx.stream.identifier.startsWith(StackOverflowRootStream.ANSWERS_TO_QUESTION)) {
    await processAnswerStream(ctx)
  } else {
    await ctx.abortRunWithError(`Unknown stream type: ${ctx.stream.identifier}`)
  }
}

export default handler
