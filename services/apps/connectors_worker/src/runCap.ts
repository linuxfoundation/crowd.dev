import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import type { IAdmissionResult } from './types'

function isLiveRun(unit: IClaimedUnit, liveUnitIds: Set<string>): boolean {
  return unit.leaseLapsed && liveUnitIds.has(unit.id)
}

export function releaseOrphan(
  unit: IClaimedUnit,
  caps: Record<string, number>,
  liveUnitIds: Set<string>,
): IClaimedUnit {
  const isOrphan =
    unit.leaseLapsed && caps[unit.platform] !== undefined && !liveUnitIds.has(unit.id)
  return isOrphan ? { ...unit, leaseLapsed: false } : unit
}

export function applyRunCaps(
  units: IClaimedUnit[],
  claimedUnits: IClaimedUnit[],
  caps: Record<string, number>,
  running: Record<string, number>,
  liveUnitIds: Set<string>,
): IAdmissionResult {
  const freeSlots: Record<string, number> = {}
  for (const [platform, cap] of Object.entries(caps)) {
    freeSlots[platform] = cap - (running[platform] ?? 0)
  }
  for (const unit of claimedUnits) {
    if (caps[unit.platform] !== undefined && isLiveRun(unit, liveUnitIds)) {
      freeSlots[unit.platform] -= 1
    }
  }

  const result: IAdmissionResult = { admitted: [], deferred: [] }
  for (const unit of units) {
    if (caps[unit.platform] === undefined) {
      result.admitted.push(unit)
    } else if (!isLiveRun(unit, liveUnitIds) && freeSlots[unit.platform] > 0) {
      freeSlots[unit.platform] -= 1
      result.admitted.push(unit)
    } else {
      result.deferred.push(releaseOrphan(unit, caps, liveUnitIds))
    }
  }
  return result
}
