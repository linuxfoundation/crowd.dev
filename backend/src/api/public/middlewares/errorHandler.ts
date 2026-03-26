import type { ErrorRequestHandler, NextFunction, Request, Response } from 'express'
import {
  InsufficientScopeError as Auth0InsufficientScopeError,
  UnauthorizedError as Auth0UnauthorizedError,
} from 'express-oauth2-jwt-bearer'

import { HttpError, InsufficientScopeError, InternalError, UnauthorizedError } from '@crowd/common'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

/**
 * Converts errors to structured JSON: `{ error: { code, message } }`.
 * Defaults to 500 Internal Error for unhandled errors.
 */
export const errorHandler: ErrorRequestHandler = (
  error: any,
  req: Request,
  res: Response,
  _next: NextFunction,
) => {
  if (error instanceof HttpError) {
    res.status(error.status).json(error.toJSON())
    return
  }

  if (error instanceof Auth0InsufficientScopeError) {
    const httpErr = new InsufficientScopeError(error.message || undefined)
    res.status(httpErr.status).json(httpErr.toJSON())
    return
  }

  if (error instanceof Auth0UnauthorizedError) {
    const httpErr = new UnauthorizedError(error.message || undefined)
    res.status(httpErr.status).json(httpErr.toJSON())
    return
  }

  req.log.error(
    { error, url: req.url, method: req.method, query: req.query, body: req.body },
    'Unhandled error in public API',
  )

  sendSlackNotification(
    SlackChannel.CDP_ALERTS,
    SlackPersona.ERROR_REPORTER,
    `Public API Error 500: ${req.method} ${req.url}`,
    [
      {
        title: 'Request',
        text: `*Method:* \`${req.method}\`\n*URL:* \`${req.url}\``,
      },
      {
        title: 'Error',
        text: `*Name:* \`${error?.name || 'Unknown'}\`\n*Message:* ${error?.message || 'No message'}`,
      },
      ...(error?.stack
        ? [
            {
              title: 'Stack Trace',
              text: `\`\`\`${error.stack.substring(0, 2700)}\`\`\``,
            },
          ]
        : []),
    ],
  )

  const unknownError = new InternalError()
  res.status(unknownError.status).json(unknownError.toJSON())
}
