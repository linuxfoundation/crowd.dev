import { OsspckgsJobKind, getLastSuccessfulSnapshot } from '@crowd/data-access-layer'

import { getPackagesDb } from '../../db'

export interface GetLastSnapshotInput {
  jobKind: OsspckgsJobKind
}

export interface GetLastSnapshotOutput {
  snapshotAt: string | null
}

export async function getLastSnapshot(input: GetLastSnapshotInput): Promise<GetLastSnapshotOutput> {
  const qx = await getPackagesDb()
  const date = await getLastSuccessfulSnapshot(qx, input.jobKind)
  return { snapshotAt: date ? date.toISOString().slice(0, 10) : null }
}
