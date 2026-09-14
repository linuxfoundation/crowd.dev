import { mergeJobTableRowCounts } from '@crowd/data-access-layer'

import { getPackagesDb } from '../../db'

export async function setJobStep(input: { jobId: number; step: string }): Promise<void> {
  const qx = await getPackagesDb()
  await mergeJobTableRowCounts(qx, input.jobId, { 'meta:step': input.step })
}
