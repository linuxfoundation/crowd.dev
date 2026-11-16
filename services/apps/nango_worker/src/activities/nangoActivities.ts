import {
  findIntegrationDataForNangoWebhookProcessing,
  getNangoCursor,
  setNangoCursor,
  updateNangoCursorLastCheckedAt,
} from '@crowd/data-access-layer/src/integrations'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getChildLogger } from '@crowd/logging'
import {
  ALL_NANGO_INTEGRATIONS,
  NangoIntegration,
  getNangoCloudRecords,
  initNangoCloudClient,
} from '@crowd/nango'
import { IntegrationResultType } from '@crowd/types'

import { svc } from '../main'
import { IProcessNangoWebhookArguments } from '../types'

export async function processNangoWebhook(
  args: IProcessNangoWebhookArguments,
): Promise<string | undefined> {
  let logger = getChildLogger(processNangoWebhook.name, svc.log, {
    provider: args.providerConfigKey,
    connectionId: args.connectionId,
    model: args.model,
  })

  if (!ALL_NANGO_INTEGRATIONS.includes(args.providerConfigKey as NangoIntegration)) {
    logger.info({ providerConfigKey: args.providerConfigKey }, 'Skipping non-Nango integration!')
    return
  }

  if (args.providerConfigKey === NangoIntegration.GITHUB) {
    logger.info('Skipping GitHub records: GitHub syncs through the connectors pipeline')
    return
  }

  const integration = await findIntegrationDataForNangoWebhookProcessing(
    dbStoreQx(svc.postgres.reader),
    args.connectionId,
  )

  if (!integration) {
    svc.log.warn(
      { connectionId: args.connectionId, provider: args.providerConfigKey },
      'Integration not found!',
    )
    return
  }

  logger = getChildLogger(processNangoWebhook.name, logger, {
    integrationId: integration.id,
  })

  let cursor = args.nextPageCursor
  let existingCursor = false
  if (!cursor) {
    const existingCursorValue = await getNangoCursor(
      dbStoreQx(svc.postgres.reader),
      integration.id,
      args.connectionId,
      args.model,
    )
    if (existingCursorValue && !['<no-cursor>', '<no-records>'].includes(existingCursorValue)) {
      cursor = existingCursorValue
      existingCursor = true
    }
  }

  await initNangoCloudClient()

  const records = await getNangoCloudRecords(
    args.providerConfigKey as NangoIntegration,
    args.connectionId,
    args.model,
    cursor,
  )

  const repo = new IntegrationStreamRepository(svc.postgres.writer, logger)

  if (records.records.length > 0) {
    logger.info(`Processing ${records.records.length} nango records!`)
    for (const record of records.records) {
      // process record
      const resultId = await repo.publishExternalResult(integration.id, {
        type: IntegrationResultType.ACTIVITY,
        // github uses public.repositories via findSegmentsForRepos() to determine segmentId
        segmentId:
          args.providerConfigKey !== NangoIntegration.GITHUB ? integration.segmentId : undefined,
        data: record.activity,
      })
      await svc.dataSinkWorkerEmitter.triggerResultProcessing(
        resultId,
        resultId,
        args.syncType === 'INITIAL',
      )
    }

    if (records.nextCursor) {
      await setNangoCursor(
        dbStoreQx(svc.postgres.writer),
        integration.id,
        args.connectionId,
        args.providerConfigKey,
        args.model,
        records.nextCursor,
      )

      return records.nextCursor
    } else {
      let cursor = '<no-cursor>'
      const lastRecord = records.records[records.records.length - 1]
      if (lastRecord.metadata?.cursor) {
        cursor = lastRecord.metadata.cursor
      }

      // if we dont have a cursor but we have an existing one we keep existing one
      // if we have a cursor from the last record we also set it
      if ((cursor === '<no-cursor>' && !existingCursor) || (cursor && cursor !== '<no-cursor>')) {
        await setNangoCursor(
          dbStoreQx(svc.postgres.writer),
          integration.id,
          args.connectionId,
          args.providerConfigKey,
          args.model,
          cursor,
        )
      }
    }
  } else if (!existingCursor) {
    // only update if we don't have an existing cursor
    await setNangoCursor(
      dbStoreQx(svc.postgres.writer),
      integration.id,
      args.connectionId,
      args.providerConfigKey,
      args.model,
      '<no-records>',
    )
  }

  // Update lastCheckedAt after successful processing
  await updateNangoCursorLastCheckedAt(
    dbStoreQx(svc.postgres.writer),
    integration.id,
    args.connectionId,
    args.model,
  )
}
