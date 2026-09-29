import bodyParser from 'body-parser'
import type { Application, Request, Response } from 'express'

import { getSlackBotConfig } from '@crowd/slack'

import { SLACK_CONFIG } from '../../conf/index'
import { safeWrap } from '../../middlewares/errorMiddleware'

// Mounted directly on the app, ahead of the rate limiter and tenant/segment
// middleware, so Slack's 3-second acknowledgement window isn't spent on
// unrelated shared middleware.
export function mountInteractivityRoute(app: Application): void {
  if (!getSlackBotConfig().signingSecret) {
    return
  }

  const captureRawBody = (req: Request, _res: Response, buf: Buffer) => {
    req.rawBody = buf
  }

  app.post(
    '/slack/interactivity',
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
