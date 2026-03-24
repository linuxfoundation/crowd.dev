import bunyanMiddleware from 'bunyan-middleware'
import cors from 'cors'
import express, {
  ErrorRequestHandler,
  NextFunction,
  Request,
  RequestHandler,
  Response,
} from 'express'

import { HttpError } from '@crowd/common'
import { Logger, getChildLogger, getServiceLogger } from '@crowd/logging'
import { ALL_NANGO_INTEGRATIONS, INangoWebhookPayload, NangoIntegration } from '@crowd/nango'
import { telemetryExpressMiddleware } from '@crowd/telemetry'
import { TEMPORAL_CONFIG, WorkflowIdReusePolicy, getTemporalClient } from '@crowd/temporal'

const log = getServiceLogger()

setImmediate(async () => {
  const temporal = await getTemporalClient(TEMPORAL_CONFIG())

  const app = express()

  app.use('/health', async (req, res) => {
    res.sendStatus(200)
  })

  app.use(telemetryExpressMiddleware('webhook.request.duration'))
  app.use(cors({ origin: true }))
  app.use(express.json({ limit: '5mb' }))
  app.use(errorMiddleware())
  app.use(loggingMiddleware(log))

  app.post(
    '/nango/webhook',
    asyncWrap(async (req, res) => {
      const payload: INangoWebhookPayload = req.body

      if (!ALL_NANGO_INTEGRATIONS.includes(payload.providerConfigKey as NangoIntegration)) {
        req.log.warn(
          { connectionId: payload.connectionId, providerConfigKey: payload.providerConfigKey },
          'Ignoring nango webhook!',
        )
        res.sendStatus(204)
        return
      }

      req.log.info(
        { connectionId: payload.connectionId, provider: payload.providerConfigKey },
        'Received nango webhook!',
      )

      await temporal.workflow.start('processNangoWebhook', {
        taskQueue: 'nango',
        workflowId: `nango-webhook/${payload.providerConfigKey}/${payload.connectionId}/${payload.model}`,
        workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
        retry: {
          maximumAttempts: 10,
        },
        args: [payload],
      })

      res.sendStatus(204)
    }),
  )

  app.listen(8084, () => {
    log.info(`Nango Webhook API listening on port 8084!`)
  })
})

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
