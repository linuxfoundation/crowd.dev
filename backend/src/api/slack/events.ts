import type { Request, Response } from 'express'
import { z } from 'zod'

import { runRequestClassificationBot } from '@/services/slack/requestClassificationBot'
import { validateOrThrow } from '@/utils/validation'

import { verifySlackSignature } from './verifySignature'

const URL_VERIFICATION_TYPE = 'url_verification'
const EVENT_CALLBACK_TYPE = 'event_callback'
const APP_MENTION_EVENT_TYPE = 'app_mention'
const RETRY_HEADER = 'x-slack-retry-num'

const payloadSchema = z.discriminatedUnion('type', [
  z.object({ type: z.literal(URL_VERIFICATION_TYPE), challenge: z.string() }),
  z.object({
    type: z.literal(EVENT_CALLBACK_TYPE),
    event: z.object({
      type: z.string(),
      text: z.string().optional(),
      channel: z.string().optional(),
      ts: z.string().optional(),
      thread_ts: z.string().optional(),
      bot_id: z.string().optional(),
    }),
  }),
])

type EventPayload = z.infer<typeof payloadSchema>

function dispatchAppMention(
  payload: Extract<EventPayload, { type: 'event_callback' }>,
  req: Request,
) {
  const { event } = payload
  const isUserMention = event.type === APP_MENTION_EVENT_TYPE && !event.bot_id
  if (!isUserMention || !event.channel || !event.ts) {
    return
  }

  runRequestClassificationBot({
    text: event.text ?? '',
    channelId: event.channel,
    threadTs: event.thread_ts ?? event.ts,
    options: { log: req.log },
  }).catch((err) => req.log.error(err, 'Slack request bot failed unexpectedly!'))
}

// Mounted ahead of responseHandlerMiddleware, so errors are handled here
// directly instead of via the global errorMiddleware.
export default async (req: Request, res: Response) => {
  if (!verifySlackSignature(req)) {
    req.log.warn('Received unverified Slack event!')
    res.sendStatus(200)
    return
  }

  try {
    const payload = validateOrThrow(payloadSchema, req.body)

    if (payload.type === URL_VERIFICATION_TYPE) {
      res.json({ challenge: payload.challenge })
      return
    }

    res.sendStatus(200)

    if (req.headers[RETRY_HEADER]) {
      return
    }

    dispatchAppMention(payload, req)
  } catch (err) {
    req.log.error(err, 'Error processing Slack event!')
    res.sendStatus(200)
  }
}
