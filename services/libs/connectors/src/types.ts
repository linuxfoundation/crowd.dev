import type { ZodType } from 'zod'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp, ResponseInterpreter } from './http/client'
import type { BudgetProbe, TokenMinter, TokenPool } from './pool/tokenPool'

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
  hasRunBudget: () => boolean
  http: ConnectorHttp
  log: Logger
}

export interface SyncOutcome {
  complete: boolean
}

export interface SyncDefinition {
  name: string
  cadenceMinutes: number
  schema: ZodType<Record<string, unknown>>
  run: (ctx: SyncContext) => Promise<SyncOutcome>
}

export interface PoolPreparation {
  preferredEntryId?: string
}

export interface Manifest {
  platform: string
  syncs: SyncDefinition[]
  discover: (credential: Credential) => Promise<Channel[]>
  preparePool?: (credential: Credential, pool: TokenPool) => Promise<PoolPreparation>
  mintToken?: (credential: Credential) => TokenMinter
  probeBudget?: BudgetProbe
  interpretResponse?: ResponseInterpreter
}
