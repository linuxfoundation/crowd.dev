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

export interface ILifecycleChannel {
  channelName: string
  hasDecommissioned: boolean
  hasEmitting: boolean
}

export interface ISyncRunProgress {
  watermark: Record<string, unknown>
  emittedCount: number
}

export interface ISyncRunSuccess extends ISyncRunProgress {
  complete: boolean
}

export type CountWindow = { kind: 'range'; from: string; to: string } | { kind: 'all-time' }

export type ValidationTarget =
  | { kind: 'units'; unitIds: string[] }
  | { kind: 'channel'; channelName: string; integrationId?: string }
  | { kind: 'channelId'; integrationId: string; channelId: string }
  | { kind: 'integration'; integrationId: string }
  | { kind: 'platform'; platform: string }

export interface IValidationChannel {
  integrationId: string
  channelId: string
  channelName: string
}

export interface IValidationUnit extends Pick<
  ISyncUnit,
  'id' | 'integrationId' | 'platform' | 'channelId' | 'channelName' | 'syncName'
> {
  validationCounterKeys: string[] | null
  validationEnabled: boolean
}
