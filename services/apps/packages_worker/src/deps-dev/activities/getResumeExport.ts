import { getIngestJobForResume } from '@crowd/data-access-layer'

import { getPackagesDb } from '../../db'

export interface GetResumeExportInput {
  jobId: number
}

export interface GetResumeExportOutput {
  jobId: number
  jobKind: string
  status: string
  syncMode: string
  gcsPrefix: string | null
  progressDone: number
  progressTotal: number
  rowCountPg: number
  ecosystems: string[] | null
  fill: boolean
}

// Pure fetch of a prior job's resume-relevant fields (export path, status, file-load progress,
// rows merged). Returns null if the job id does not exist. All validation — kind, status, presence
// of an export, and that the parquet files still exist — is done by the caller (ingestDependencies)
// so it can fail fast with non-retryable errors instead of retrying a bad-input activity.
export async function getResumeExport(
  input: GetResumeExportInput,
): Promise<GetResumeExportOutput | null> {
  const qx = await getPackagesDb()
  const job = await getIngestJobForResume(qx, input.jobId)
  if (!job) {
    return null
  }
  return {
    jobId: job.id,
    jobKind: job.jobKind,
    status: job.status,
    syncMode: job.syncMode,
    gcsPrefix: job.gcsPrefix,
    progressDone: job.progressDone,
    progressTotal: job.progressTotal,
    rowCountPg: job.rowCountPg,
    ecosystems: job.ecosystems,
    fill: job.fill,
  }
}
