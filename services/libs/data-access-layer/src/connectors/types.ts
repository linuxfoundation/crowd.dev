export type SyncUnitStatus = 'active' | 'paused' | 'dead_letter' | 'decommissioned'

export interface ISyncUnit {
  id: string
  integrationId: string
  platform: string
  channelId: string
  channelName: string
  syncName: string
  status: SyncUnitStatus
  nextRunAt: string
  lockedAt: string | null
  lastRunAt: string | null
  lastSuccessAt: string | null
  consecutiveFailures: number
  lastErrorClass: string | null
  lastErrorMessage: string | null
  lastRunComplete: boolean | null
  watermark: Record<string, unknown> | null
  emittedCount: number | null
  emitEnabled: boolean
}

export interface IShadowRecord {
  type: string
  sourceId: string
  occurredAt: string
  data: Record<string, unknown>
  segmentId: string
  integrationId: string
}

export type SyncUnitUpsert = Pick<
  ISyncUnit,
  'integrationId' | 'platform' | 'channelId' | 'channelName' | 'syncName'
> & {
  watermark?: Record<string, unknown> | null
  // Insert-only: re-running an upsert never changes these on an existing unit.
  emitEnabled?: boolean
  initialDelaySeconds?: number
}

export type IClaimedUnit = Pick<
  ISyncUnit,
  'id' | 'integrationId' | 'platform' | 'syncName' | 'channelId' | 'channelName'
> & { leaseLapsed: boolean }

export type IShadowDiffUnit = Pick<ISyncUnit, 'id' | 'integrationId' | 'channelName' | 'syncName'>

export type IShadowDiffChannelCursor = Pick<ISyncUnit, 'integrationId' | 'channelName'>

export interface IShadowDiffUnitsPage {
  channelLimit: number
  after?: IShadowDiffChannelCursor
}

export interface ISyncRunProgress {
  watermark: Record<string, unknown>
  emittedCount: number
}

export interface ISyncRunSuccess extends ISyncRunProgress {
  complete: boolean
}
