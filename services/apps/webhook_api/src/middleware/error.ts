import { ErrorRequestHandler, NextFunction, Request, Response } from 'express'

import { HttpError } from '@crowd/common'

import { ApiRequest } from '.'

export const asyncWrap =
  (fn: (req: ApiRequest, res: Response, next: NextFunction) => Promise<void>) =>
  (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req as ApiRequest, res, next)).catch(next)
  }

export const errorMiddleware = (): ErrorRequestHandler => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  return (err, req, res, _next) => {
    const request = req as ApiRequest

    if (err instanceof HttpError) {
      request.log.error(err, { statusCode: err.status }, 'HTTP error occurred!')
      res.status(err.status).json(err.toJSON())
    } else {
      request.log.error(err, 'Unknown error occured!')
      res.status(500).send('Internal Server Error')
    }
  }
}
