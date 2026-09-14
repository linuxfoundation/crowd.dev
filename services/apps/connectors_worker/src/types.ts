import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

export type StartRunResult = 'started' | 'alreadyRunning'

export interface IAdmissionResult {
  admitted: IClaimedUnit[]
  deferred: IClaimedUnit[]
}

export interface IDispatchCounts {
  claimed: number
  admitted: number
  deferred: number
  started: number
  alreadyRunning: number
  failed: number
  durationMs: number
}
