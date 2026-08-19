import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

export type StartRunResult = 'started' | 'alreadyRunning'

export interface IAdmissionResult {
  admitted: IClaimedUnit[]
  deferred: IClaimedUnit[]
}
