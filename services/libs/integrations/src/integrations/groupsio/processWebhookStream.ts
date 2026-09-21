import { ProcessWebhookStreamHandler } from '../../types'
import {
  GroupsioMemberJoinData, //   GroupsioMessageData,
  GroupsioMemberLeftData,
  GroupsioPublishData,
  GroupsioPublishDataType,
  GroupsioWebhookEventType,
  GroupsioWebhookJoinPayload,
  GroupsioWebhookPayload,
} from './types'

const processWebhookJoin: ProcessWebhookStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioWebhookPayload<GroupsioWebhookJoinPayload>
  const payload = data.data

  await ctx.processData<GroupsioPublishData<GroupsioMemberJoinData>>({
    type: GroupsioPublishDataType.MEMBER_JOIN,
    data: {
      member: payload.member_info,
      group: payload.group.name,
      joinedAt: new Date(payload.created).toISOString(),
    },
  })
}

const processWebhookLeft: ProcessWebhookStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GroupsioWebhookPayload<GroupsioWebhookJoinPayload>
  const payload = data.data

  await ctx.processData<GroupsioPublishData<GroupsioMemberLeftData>>({
    type: GroupsioPublishDataType.MEMBER_LEFT,
    data: {
      member: payload.member_info,
      group: payload.group.name,
      leftAt: new Date(payload.created).toISOString(),
    },
  })
}

// const processWebhookSentMessageAccepted: ProcessWebhookStreamHandler = async (ctx) => {}

const handler: ProcessWebhookStreamHandler = async (ctx) => {
  const { event } = ctx.stream.data as GroupsioWebhookPayload<unknown>

  //verifyWebhookSignature(data as string, data as string, ctx)

  switch (event) {
    case GroupsioWebhookEventType.JOINED: {
      await processWebhookJoin(ctx)
      break
    }

    // case GroupsioWebhookEventType.SENT_MESSAGE_ACCEPTED: {
    //   await processWebhookSentMessageAccepted(ctx)
    //   break
    // }

    case GroupsioWebhookEventType.LEFT: {
      await processWebhookLeft(ctx)
      break
    }

    default: {
      ctx.log.warn({ event }, `Unsupported Groupsio webhook type, skipping it`)
    }
  }
}

export default handler
