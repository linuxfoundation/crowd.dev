import type { Request, Response } from 'express'
import { z } from 'zod'

import { validateOrThrow } from '@/utils/validation'

import { verifySlackSignature } from './verifySignature'

const bodySchema = z.object({
  payload: z.string(),
})

const payloadSchema = z.object({
  type: z.string(),
  actions: z.array(z.object({ action_id: z.string() })).optional(),
})

export default async (req: Request, res: Response) => {
  if (!verifySlackSignature(req)) {
    req.log.warn('Received unverified Slack interactivity payload!')
    res.sendStatus(200)
    return
  }

  const { payload: rawPayload } = validateOrThrow(bodySchema, req.body)
  const payload = validateOrThrow(payloadSchema, JSON.parse(rawPayload))

  req.log.info(
    { type: payload.type, actionIds: payload.actions?.map((a) => a.action_id) },
    'Received Slack interactivity payload.',
  )

  // TODO(CM-1791): wire up the claim-button interactive handler.
  res.sendStatus(200)
}
