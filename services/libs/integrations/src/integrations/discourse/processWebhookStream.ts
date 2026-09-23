import { IProcessStreamContext, ProcessWebhookStreamHandler } from '../../types'
import { getDiscourseUserByUsername } from './api/getUser'
import { usernameIsBot } from './processStream'
import {
  DiscourseConnectionParams,
  DiscourseDataType,
  DiscoursePublishNotificationWebhookData,
  DiscoursePublishPostData,
  DiscoursePublishUserWebhookData,
  DiscourseWebhookNotification,
  DiscourseWebhookPost,
  DiscourseWebhookStreamData,
  DiscourseWebhookType,
  DiscourseWebhookUser,
} from './types'

const processPostCreatedWebhook: ProcessWebhookStreamHandler = async (ctx) => {
  const { data } = ctx.stream.data as DiscourseWebhookStreamData

  const post = (data as DiscourseWebhookPost).post

  const { forumHostname, apiKey, apiUsername } = ctx.integration
    .settings as DiscourseConnectionParams

  if (usernameIsBot(post.username)) {
    return
  }
  const user = await getDiscourseUserByUsername(
    {
      forumHostname,
      apiKey,
      apiUsername,
    },
    { username: post.username },
    ctx as IProcessStreamContext,
  )

  await ctx.processData<DiscoursePublishPostData>({
    type: DiscourseDataType.POST,
    user,
    post,
    topicId: post.topic_id,
    topicSlug: post.topic_slug,
    topicTitle: post.topic_title,
  })
}

const processUserCreatedWebhook: ProcessWebhookStreamHandler = async (ctx) => {
  const { data } = ctx.stream.data as DiscourseWebhookStreamData

  const user = (data as DiscourseWebhookUser).user

  if (usernameIsBot(user.username)) {
    return
  }

  await ctx.processData<DiscoursePublishUserWebhookData>({
    type: DiscourseDataType.USER_WEBHOOK,
    user,
  })
}

const processLikedAPostWebhook: ProcessWebhookStreamHandler = async (ctx) => {
  const { data } = ctx.stream.data as DiscourseWebhookStreamData
  const notification = (data as DiscourseWebhookNotification).notification

  const { forumHostname, apiKey, apiUsername } = ctx.integration
    .settings as DiscourseConnectionParams

  const username = notification.data.username
    ? notification.data.username
    : notification.data.original_username
  const channel = notification.fancy_title
    ? notification.fancy_title
    : notification.data.topic_title

  if (usernameIsBot(username)) {
    return
  }

  const user = await getDiscourseUserByUsername(
    {
      forumHostname,
      apiKey,
      apiUsername,
    },
    { username },
    ctx as IProcessStreamContext,
  )

  if (!user) {
    return
  }

  await ctx.processData<DiscoursePublishNotificationWebhookData>({
    type: DiscourseDataType.NOTIFICATION_WEBHOOK,
    user,
    notification,
    channel,
  })
}

const handler: ProcessWebhookStreamHandler = async (ctx) => {
  const { event, data } = ctx.stream.data as DiscourseWebhookStreamData

  switch (event) {
    case DiscourseWebhookType.POST_CREATED:
      await processPostCreatedWebhook(ctx)
      break
    case DiscourseWebhookType.USER_CREATED:
      await processUserCreatedWebhook(ctx)
      break
    case DiscourseWebhookType.LIKED_A_POST: {
      const localData = data as DiscourseWebhookNotification
      if (localData.notification.notification_type === 5) {
        await processLikedAPostWebhook(ctx)
      }
      break
    }
    default:
      ctx.log.warn(
        {
          event,
          data,
        },
        'No record created for event!',
      )
  }
}

export default handler
