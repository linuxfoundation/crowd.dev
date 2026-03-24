import express from 'express'

import { BadRequestError } from '@crowd/common'
import { WebhooksRepository } from '@crowd/data-access-layer/src/old/apps/webhook_api/webhooks.repo'
import { PlatformType, WebhookType } from '@crowd/types'

import { asyncWrap } from '../middleware/error'
import { verifyWebhookSignature } from '../utils/crypto'

export const installDiscourseRoutes = async (app: express.Express) => {
  app.post(
    '/discourse/:tenantId/',
    asyncWrap(async (req, res) => {
      const signature = req.headers['x-discourse-event-signature']
      if (!signature) {
        throw new BadRequestError('Missing signature header!')
      }

      const eventId = req.headers['x-discourse-event-id']
      const eventType = req.headers['x-discourse-event-type']
      const event = req.headers['x-discourse-event']
      const data = req.body

      const repo = new WebhooksRepository(req.dbStore, req.log)
      const integration = await repo.findIntegrationByPlatform(PlatformType.DISCOURSE)

      if (integration) {
        req.log.info({ integrationId: integration.id }, 'Incoming Discourse Webhook!')

        if (
          !verifyWebhookSignature(
            JSON.stringify(data),
            integration.settings.webhookSecret,
            signature as string,
          )
        ) {
          req.log.warn({ signature }, 'Discourse Webhook signature verification failed!')
          res.sendStatus(200)
          return
        }

        const id = await repo.createIncomingWebhook(integration.id, WebhookType.DISCOURSE, {
          signature,
          eventId,
          eventType,
          event,
          data,
          date: new Date().toISOString(),
        })

        await req.emitters.integrationStreamWorker.triggerWebhookProcessing(
          integration.platform,
          id,
        )

        res.sendStatus(204)
      } else {
        req.log.warn(
          { tenantId: req.params.tenantId },
          'Discourse integration not found for incoming webhook!',
        )
        res.sendStatus(200)
      }
    }),
  )
}
