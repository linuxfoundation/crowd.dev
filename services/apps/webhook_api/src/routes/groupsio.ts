import express from 'express'

import { BadRequestError } from '@crowd/common'
import { WebhooksRepository } from '@crowd/data-access-layer/src/old/apps/webhook_api/webhooks.repo'
import { WebhookType } from '@crowd/types'

import { asyncWrap } from '../middleware/error'

export const installGroupsIoRoutes = async (app: express.Express) => {
  app.post(
    '/groupsio',
    asyncWrap(async (req, res) => {
      const signature = req.headers['x-groupsio-signature']
      const event = req.headers['x-groupsio-action']
      const data = req.body

      // TODO: Validate signature - need to get secret from groups io for Linux
      const groupName = data?.group?.name

      if (!groupName) {
        throw new BadRequestError('Missing group name!')
      }

      const repo = new WebhooksRepository(req.dbStore, req.log)

      const integration = await repo.findGroupsIoIntegrationByGroupName(groupName)

      if (integration) {
        req.log.info({ integrationId: integration.id }, 'Incoming Groups.io Webhook!')

        const result = await repo.createIncomingWebhook(integration.id, WebhookType.GROUPSIO, {
          signature,
          event,
          data,
        })
        req.log.info({ integrationId: integration.id }, 'Trigerring Webhook Processing!')
        await req.emitters.integrationStreamWorker.triggerWebhookProcessing(
          integration.platform,
          result,
        )
        req.log.info({ integrationId: integration.id }, 'Webhook processed!')
        res.sendStatus(204)
      } else {
        req.log.error({ event, groupName }, 'No integration found for incoming Groups.io Webhook!')
        res.status(200).send({
          message: 'No integration found for incoming Groups.io Webhook!',
        })
      }
    }),
  )
}
