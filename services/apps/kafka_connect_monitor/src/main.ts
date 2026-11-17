import bunyanMiddleware from 'bunyan-middleware'
import express, { ErrorRequestHandler, Request, RequestHandler } from 'express'

import { Logger, getChildLogger, getServiceLogger } from '@crowd/logging'

import { installConnectorHealthRoutes } from './routes/health'

const log = getServiceLogger()
const PORT = 8085

setImmediate(async () => {
  const app = express()

  app.use('/health', async (req, res) => {
    res.sendStatus(200)
  })

  app.use(express.json())
  app.use(loggingMiddleware(log))

  // Install routes
  installConnectorHealthRoutes(app, log)

  app.use(errorMiddleware())

  app.listen(PORT, () => {
    log.info(`Kafka Connect Monitor listening on port ${PORT}!`)
  })
})

export const errorMiddleware = (): ErrorRequestHandler => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  return (err, req, res, _next) => {
    const request = req as ApiRequest

    request.log.error(err, 'Error occurred!')
    res.status(500).send('Internal Server Error')
  }
}

export interface ApiRequest extends Request {
  log: Logger
}

export const loggingMiddleware = (log: Logger): RequestHandler => {
  return bunyanMiddleware({
    headerName: 'x-request-id',
    propertyName: 'requestId',
    logName: `requestId`,
    logger: getChildLogger('apiRequest', log),
    level: 'trace',
  })
}
