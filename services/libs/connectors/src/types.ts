import type { ZodType } from 'zod'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from './http/client'

export interface Channel {
  channelId: string
  channelName: string
}

export interface GithubAppCredentialData {
  appId: string
  privateKey: string
}

// POC only: single variant; becomes a discriminated union (token, oauth2, ...)
// as more connectors land
export interface Credential {
  platform: string
  kind: 'github-app'
  data: GithubAppCredentialData
}

export interface SyncContext {
  channel: Channel
  watermark: Record<string, unknown> | null
  emit: (records: unknown[]) => Promise<void>
  commitWatermark: (watermark: Record<string, unknown>) => Promise<void>
  http: ConnectorHttp
  log: Logger
}

export interface SyncDefinition {
  name: string
  cadenceMinutes: number
  schema: ZodType<Record<string, unknown>>
  run: (ctx: SyncContext) => Promise<void>
}

export interface Manifest {
  platform: string
  syncs: SyncDefinition[]
  discover: (credential: Credential) => Promise<Channel[]>
}
