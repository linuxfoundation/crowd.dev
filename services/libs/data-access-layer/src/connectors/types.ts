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
}

export type SyncUnitUpsert = Pick<
  ISyncUnit,
  'integrationId' | 'platform' | 'channelId' | 'channelName' | 'syncName'
>

export type IClaimedUnit = Pick<
  ISyncUnit,
  'id' | 'integrationId' | 'platform' | 'syncName' | 'channelId' | 'channelName'
>

export interface ISyncRunProgress {
  watermark: Record<string, unknown>
  emittedCount: number
}

export interface ISyncRunSuccess extends ISyncRunProgress {
  complete: boolean
}
