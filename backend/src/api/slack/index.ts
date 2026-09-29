import bodyParser from 'body-parser'
import type { Application, NextFunction, Request, Response } from 'express'

import { getSlackBotConfig } from '@crowd/slack'

import { SLACK_CONFIG } from '../../conf/index'
import { safeWrap } from '../../middlewares/errorMiddleware'
import { createRateLimiter } from '../apiRateLimiter'

// Mounted ahead of the shared rate limiter and tenant/segment middleware
// to protect Slack's 3-second acknowledgement window; keeps its own limiter.
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const handleParserError = (err: Error, req: Request, res: Response, _next: NextFunction) => {
    req.log.error(err, 'Error parsing Slack interactivity payload!')
    res.sendStatus(200)
  }

  app.post(
    '/api/v1/slack/interactivity',
    interactivityRateLimiter,
    bodyParser.urlencoded({ limit: '5mb', extended: true, verify: captureRawBody }),
    handleParserError,
    require('./interactivity').default,
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
