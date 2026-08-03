import type { ErrorRequestHandler, NextFunction, Request, Response } from 'express'
import {
  InsufficientScopeError as Auth0InsufficientScopeError,
  UnauthorizedError as Auth0UnauthorizedError,
} from 'express-oauth2-jwt-bearer'

import {
  ConflictError,
  HttpError,
  InsufficientScopeError,
  InternalError,
  UnauthorizedError,
} from '@crowd/common'

import { alertOnce } from '@/api/public/alerts/alertOnce'

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
    void alertOnce(req, {
      status: error.status,
      code: error.code,
      message: error.message,
      name: error.name,
      context: error instanceof ConflictError ? error.context : undefined,
    })
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
    {
      error: { name: error?.name, message: error?.message, stack: error?.stack },
      url: req.url,
      method: req.method,
      query: req.query,
      body: req.body,
    },
    'Unhandled error in public API',
  )

  void alertOnce(req, {
    status: 500,
    code: 'INTERNAL_ERROR',
    message: error?.message || 'No message',
    name: error?.name || 'Unknown',
    stack: error?.stack,
  })

  const unknownError = new InternalError()
  res.status(unknownError.status).json(unknownError.toJSON())
}
