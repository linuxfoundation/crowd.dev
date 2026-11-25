import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import type { IAdmissionResult } from './types'

export function applyRunCaps(
  units: IClaimedUnit[],
  caps: Record<string, number>,
  running: Record<string, number>,
): IAdmissionResult {
  const inFlight: Record<string, number> = { ...running }
  for (const unit of units) {
    if (unit.leaseLapsed && caps[unit.platform] !== undefined) {
      inFlight[unit.platform] = (inFlight[unit.platform] ?? 0) + 1
    }
  }

  const result: IAdmissionResult = { admitted: [], deferred: [] }
  for (const unit of units) {
    const cap = caps[unit.platform]
    if (cap === undefined) {
      result.admitted.push(unit)
    } else if (!unit.leaseLapsed && (inFlight[unit.platform] ?? 0) < cap) {
      inFlight[unit.platform] = (inFlight[unit.platform] ?? 0) + 1
      result.admitted.push(unit)
    } else {
      result.deferred.push(unit)
    }
  }
  return result
}
