import type Nango from '@nangohq/frontend' assert { 'resolution-mode': 'require' }
import type { Nango as BackendNango, SyncStatus } from '@nangohq/node' assert {
  'resolution-mode': 'require',
}
import type { ApiPublicConnection, ApiPublicConnectionFull } from '@nangohq/types' assert {
  'resolution-mode': 'require',
}
import axios from 'axios'

import { SERVICE, timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import {
  INangoClientConfig,
  INangoCloudSessionToken,
  INangoResult,
  NANGO_INTEGRATION_CONFIG,
  NangoIntegration,
} from './types'
import { toRecord } from './utils'

const log = getServiceChildLogger('nango')

const MAX_RETRIES = 10

export type { SyncStatus } from '@nangohq/node' assert { 'resolution-mode': 'require' }

export type { ApiPublicConnection, ApiPublicConnectionFull } from '@nangohq/types' assert {
  'resolution-mode': 'require',
}

/* eslint-disable @typescript-eslint/no-explicit-any */

let backendClient: BackendNango | undefined = undefined

const DEFAULT_NANGO_FETCH_LIMIT = 100

let config: INangoClientConfig | undefined | null = undefined
export const NANGO_CLOUD_CONFIG = () => {
  if (!config && config !== null) {
    if (process.env.NANGO_CLOUD_SECRET_KEY) {
      config = {
        secretKey: process.env.NANGO_CLOUD_SECRET_KEY,
        integrations: process.env.NANGO_CLOUD_INTEGRATIONS.split(','),
      }
    } else {
      config = null
    }
  }

  return config
}

function ensureBackendClient() {
  if (!backendClient) {
    throw new Error('Nango client not initialized')
  }
}

export const initNangoCloudClient = async (): Promise<void> => {
  if (!backendClient) {
    const config = NANGO_CLOUD_CONFIG()
    if (config) {
      log.info('Initializing Nango cloud client...')
      const backendModule = await import('@nangohq/node')
      backendClient = new backendModule.Nango({ secretKey: config.secretKey })
    } else {
      throw new Error('Nango cloud client env variables not set!')
    }
  }
}

export const getNangoCloudSessionToken = async (): Promise<INangoCloudSessionToken> => {
  ensureBackendClient()

  const service = `cm-${SERVICE}`

  const response = await backendClient.createConnectSession({
    end_user: {
      id: service,
    },
    allowed_integrations: NANGO_CLOUD_CONFIG().integrations,
  })

  return {
    token: response.data.token,
    expiresAt: response.data.expires_at,
  }
}

export const getNangoConnectionStatus = async (
  integration: NangoIntegration,
  connectionId: string,
): Promise<SyncStatus[]> => {
  ensureBackendClient()

  try {
    log.debug(`Getting nango sync status for connection ${connectionId}`)
    const res = await backendClient.syncStatus(integration, '*', connectionId)
    return res.syncs
  } catch (error) {
    if (axios.isAxiosError(error)) {
      if (error.response?.status === 404) {
        log.warn(`Connection ${connectionId} could not be found in Nango`)
        return null
      }
    }
    throw error
  }
}

export const getNangoConnections = async (): Promise<ApiPublicConnection[]> => {
  ensureBackendClient()

  const limit = 2000
  let page = 0
  let hasMore = true
  const allConnections: ApiPublicConnection[] = []

  while (hasMore) {
    const results = await backendClient.listConnections(undefined, undefined, { limit, page })
    allConnections.push(...results.connections)
    hasMore = results.connections.length === limit
    page++
  }

  return allConnections
}

export const getNangoConnectionData = async (
  integration: NangoIntegration,
  connectionId: string,
): Promise<ApiPublicConnectionFull> => {
  ensureBackendClient()

  const result = await backendClient.getConnection(integration, connectionId)

  return result
}

let frontendModule: any | undefined = undefined

export const connectNangoIntegration = async (
  integration: NangoIntegration,
  params: any,
): Promise<string> => {
  ensureBackendClient()

  log.info({ params, integration }, 'Creating a nango connection...')
  const data = await getNangoCloudSessionToken()

  if (!frontendModule) {
    frontendModule = await import('@nangohq/frontend')
  }

  const frontendClient = new frontendModule.default({
    connectSessionToken: data.token,
  }) as Nango

  const result = await frontendClient.auth(integration, params)
  return result.connectionId
}

export const createNangoConnection = async (
  integration: NangoIntegration,
  params: any,
  retries = 1,
): Promise<string> => {
  ensureBackendClient()

  log.info({ params, integration }, 'Creating a nango connection...')

  const data = await getNangoCloudSessionToken()
  if (!frontendModule) {
    frontendModule = await import('@nangohq/frontend')
  }

  const frontendClient = new frontendModule.default({
    connectSessionToken: data.token,
  }) as Nango

  try {
    const result = await frontendClient.create(integration, params)
    return result.connectionId
  } catch (err) {
    if (retries <= MAX_RETRIES) {
      await timeout(100)
      return await createNangoConnection(integration, params, retries + 1)
    }

    log.error(err, 'Error creating nango connection')
    throw err
  }
}

export const setNangoMetadata = async (
  integration: NangoIntegration,
  connectionId: string,
  metadata: Record<string, unknown>,
  retries = 1,
): Promise<void> => {
  ensureBackendClient()

  try {
    // changed setMetadata to updateMetadata since it was deprecated
    await backendClient.updateMetadata(integration, connectionId, metadata)
    // Temporary sleep to ensure metadata is fully updated (bug on Nango's side)
    await new Promise((r) => setTimeout(r, 200))
  } catch (err) {
    if (retries <= MAX_RETRIES) {
      await timeout(100)
      return await setNangoMetadata(integration, connectionId, metadata, retries + 1)
    } else {
      throw err
    }
  }
}

export const startNangoSync = async (
  integration: NangoIntegration,
  connectionId: string,
  syncs?: string[],
  fullSync = false,
  retries = 1,
): Promise<void> => {
  ensureBackendClient()
  log.info({ connectionId, integration, syncs }, 'Starting a nango sync...')

  try {
    if (!syncs) {
      syncs = Object.values(NANGO_INTEGRATION_CONFIG[integration].syncs) as string[]
    } else {
      // verify against config
      const validSyncs = Object.values(NANGO_INTEGRATION_CONFIG[integration].syncs) as string[]
      for (const sync of syncs) {
        if (!validSyncs.includes(sync)) {
          const available = validSyncs.join(', ')

          throw new Error(
            `Invalid sync: ${sync}! Integration '${integration}' supports these syncs: ${available}`,
          )
        }
      }
    }

    if (fullSync) {
      await axios.post(
        'https://api.nango.dev/sync/trigger',
        {
          provider_config_key: integration,
          connection_id: connectionId,
          syncs,
          sync_mode: 'full_refresh_and_clear_cache',
          full_resync: true,
        },
        {
          headers: {
            Authorization: `Bearer ${process.env.NANGO_CLOUD_SECRET_KEY}`,
          },
        },
      )
    } else {
      await backendClient.startSync(integration, syncs, connectionId)
    }
  } catch (err) {
    if (retries <= MAX_RETRIES) {
      await timeout(100)
      return await startNangoSync(integration, connectionId, syncs, fullSync, retries + 1)
    } else {
      log.error(err, 'Error starting nango sync!')
      throw err
    }
  }
}

export const pauseNangoSync = async (
  integration: NangoIntegration,
  connectionId: string,
  syncs: string[],
  retries = 1,
): Promise<void> => {
  ensureBackendClient()
  log.info({ connectionId, integration, syncs }, 'Pausing a nango sync...')

  const validSyncs = Object.values(NANGO_INTEGRATION_CONFIG[integration].syncs) as string[]
  for (const sync of syncs) {
    if (!validSyncs.includes(sync)) {
      throw new Error(
        `Invalid sync: ${sync}! Integration '${integration}' supports these syncs: ${validSyncs.join(', ')}`,
      )
    }
  }

  try {
    await backendClient.pauseSync(integration, syncs, connectionId)
  } catch (err) {
    if (retries <= MAX_RETRIES) {
      await timeout(100)
      return await pauseNangoSync(integration, connectionId, syncs, retries + 1)
    }
    log.error(err, 'Error pausing nango sync!')
    throw err
  }
}

export const deleteNangoConnection = async (
  integration: NangoIntegration,
  connectionId: string,
  retries = 1,
): Promise<void> => {
  ensureBackendClient()

  try {
    await backendClient.deleteConnection(integration, connectionId)
  } catch (err) {
    if (axios.isAxiosError(err)) {
      if (err.response?.status === 404) {
        return
      }

      if (
        err.response?.status === 400 &&
        err.response?.data?.error?.code === 'unknown_connection'
      ) {
        return
      }
    }

    if (retries <= MAX_RETRIES) {
      await timeout(100)
      return await deleteNangoConnection(integration, connectionId, retries + 1)
    } else {
      throw err
    }
  }
}

export const getNangoCloudRecords = async (
  integration: NangoIntegration,
  connectionId: string,
  model: string,
  cursor?: string,
  limit?: number,
  modifiedAfter?: string,
): Promise<INangoResult> => {
  ensureBackendClient()

  // verify model against config
  if (!(Object.values(NANGO_INTEGRATION_CONFIG[integration].models) as string[]).includes(model)) {
    const available = Object.values(NANGO_INTEGRATION_CONFIG[integration].models).join(', ')

    throw new Error(
      `Invalid model: ${model}! Integration '${integration}' supports these models: ${available}`,
    )
  }

  const result = await backendClient.listRecords({
    providerConfigKey: integration,
    connectionId,
    model,
    limit: limit ?? DEFAULT_NANGO_FETCH_LIMIT,
    cursor,
    modifiedAfter,
  })

  const records = result.records.map(toRecord)

  return {
    records,
    nextCursor: result.next_cursor,
  }
}
