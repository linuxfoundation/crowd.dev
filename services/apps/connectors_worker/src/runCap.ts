import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import type { IAdmissionResult } from './types'

export function applyRunCaps(
  units: IClaimedUnit[],
  claimedUnits: IClaimedUnit[],
  caps: Record<string, number>,
  running: Record<string, number>,
): IAdmissionResult {
  const freeSlots: Record<string, number> = {}
  for (const [platform, cap] of Object.entries(caps)) {
    freeSlots[platform] = cap - (running[platform] ?? 0)
  }

  const slottedLapsedIds = new Set<string>()
  for (const unit of claimedUnits) {
    if (unit.leaseLapsed && freeSlots[unit.platform] > 0) {
      freeSlots[unit.platform] -= 1
      slottedLapsedIds.add(unit.id)
    }
  }

  const result: IAdmissionResult = { admitted: [], deferred: [] }
  for (const unit of units) {
    if (caps[unit.platform] === undefined) {
      result.admitted.push(unit)
    } else if (slottedLapsedIds.has(unit.id)) {
      result.admitted.push(unit)
    } else if (!unit.leaseLapsed && freeSlots[unit.platform] > 0) {
      freeSlots[unit.platform] -= 1
      result.admitted.push(unit)
    } else {
      result.deferred.push(unit)
    }
  }
  return result
}
