import bodyParser from 'body-parser'
import type { Application, Request, Response } from 'express'

import { getSlackBotConfig } from '@crowd/slack'

import { SLACK_CONFIG } from '../../conf/index'
import { safeWrap } from '../../middlewares/errorMiddleware'
import { createRateLimiter } from '../apiRateLimiter'

// Mounted directly on the app, ahead of the shared rate limiter and
// tenant/segment middleware, so Slack's 3-second acknowledgement window
// isn't spent on unrelated shared middleware. It keeps its own rate
// limiter since it bypasses the shared one.
export function mountInteractivityRoute(app: Application): void {
  if (!getSlackBotConfig().signingSecret) {
    return
  }

  const captureRawBody = (req: Request, _res: Response, buf: Buffer) => {
    req.rawBody = buf
  }

  const interactivityRateLimiter = createRateLimiter({
    max: 200,
    windowMs: 60 * 1000,
  })

  app.post(
    '/slack/interactivity',
    interactivityRateLimiter,
    bodyParser.urlencoded({ limit: '5mb', extended: true, verify: captureRawBody }),
    safeWrap(require('./interactivity').default),
  )
}

export default (app) => {
  if (
    SLACK_CONFIG.onboardingAppId &&
    SLACK_CONFIG.onboardingAppToken &&
    SLACK_CONFIG.onboardingTeamId
  ) {
    app.post('/slack/commands', safeWrap(require('./command').default))
  }
}
