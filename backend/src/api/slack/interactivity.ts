import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  replaceWithForceOnboardingFailure,
  runForceOnboardingCommand,
} from '@/services/slack/forceOnboardingCommand'
import { FORCE_ONBOARDING_ACTION_ID } from '@/services/slack/slackActionIds'
import { validateOrThrow } from '@/utils/validation'

import { verifySlackSignature } from './verifySignature'

const bodySchema = z.object({
  payload: z.string(),
})

const payloadSchema = z.object({
  type: z.string(),
  user: z.object({ id: z.string() }).optional(),
  response_url: z.string().optional(),
  actions: z.array(z.object({ action_id: z.string(), value: z.string().optional() })).optional(),
})

type InteractivityPayload = z.infer<typeof payloadSchema>

function dispatchForceOnboarding(payload: InteractivityPayload, req: Request) {
  const action = payload.actions?.find((a) => a.action_id === FORCE_ONBOARDING_ACTION_ID)
  if (!action || !payload.user || !payload.response_url || !action.value) {
    return false
  }

  const responseUrl = payload.response_url
  runForceOnboardingCommand({
    catalogId: action.value,
    responseUrl,
    actorId: payload.user.id,
    log: req.log,
  }).catch((err) => {
    req.log.error(err, 'Force onboarding failed unexpectedly!')
    replaceWithForceOnboardingFailure(responseUrl, req.log)
  })
  return true
}

// Mounted ahead of responseHandlerMiddleware, so errors are handled here
// directly instead of via the global errorMiddleware.
export default async (req: Request, res: Response) => {
  if (!verifySlackSignature(req)) {
    req.log.warn('Received unverified Slack interactivity payload!')
    res.sendStatus(200)
    return
  }

  try {
    const { payload: rawPayload } = validateOrThrow(bodySchema, req.body)
    const payload = validateOrThrow(payloadSchema, JSON.parse(rawPayload))

    req.log.info(
      { type: payload.type, actionIds: payload.actions?.map((a) => a.action_id) },
      'Received Slack interactivity payload.',
    )

    res.sendStatus(200)

    if (payload.type === 'block_actions' && !dispatchForceOnboarding(payload, req)) {
      // TODO(CM-1791): wire up the claim-button interactive handler.
      req.log.warn('Unhandled Slack block action.')
    }
  } catch (err) {
    req.log.error(err, 'Error processing Slack interactivity payload!')
    res.sendStatus(200)
  }
}
