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
}

export type SyncUnitUpsert = Pick<
  ISyncUnit,
  'integrationId' | 'platform' | 'channelId' | 'channelName' | 'syncName'
>

export type IClaimedUnit = Pick<
  ISyncUnit,
  'id' | 'integrationId' | 'platform' | 'syncName' | 'channelId' | 'channelName'
>

export type IShadowDiffUnit = Pick<ISyncUnit, 'id' | 'integrationId' | 'channelName' | 'syncName'>

export interface ISyncRunProgress {
  watermark: Record<string, unknown>
  emittedCount: number
}

export interface ISyncRunSuccess extends ISyncRunProgress {
  complete: boolean
}
