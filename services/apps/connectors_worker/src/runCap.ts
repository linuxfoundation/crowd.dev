import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import type { IAdmissionResult } from './types'

export function addLeaseLapsedUnits(
  running: Record<string, number>,
  claimedUnits: IClaimedUnit[],
  caps: Record<string, number>,
): Record<string, number> {
  const total: Record<string, number> = { ...running }
  for (const unit of claimedUnits) {
    if (unit.leaseLapsed && caps[unit.platform] !== undefined) {
      total[unit.platform] = (total[unit.platform] ?? 0) + 1
    }
  }
  return total
}

export function applyRunCaps(
  units: IClaimedUnit[],
  caps: Record<string, number>,
  running: Record<string, number>,
): IAdmissionResult {
  const inFlight: Record<string, number> = { ...running }
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
