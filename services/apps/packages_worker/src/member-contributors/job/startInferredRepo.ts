import { ApplicationFailure } from '@temporalio/activity'

import {
  MemberContributorJobPurl,
  findMemberContributorJobPurl,
  markMemberContributorJobRunning,
} from '@crowd/data-access-layer/src/osspckgs/memberContributorJobs'

import { getPackagesDb } from '../../db'
import { MemberContributorsJobRef } from './start'

export async function startInferredRepoMemberContributorsJob(
  input: MemberContributorsJobRef,
): Promise<MemberContributorJobPurl> {
  const pkgsQx = await getPackagesDb()
  await markMemberContributorJobRunning(pkgsQx, input.jobId)
  const job = await findMemberContributorJobPurl(pkgsQx, input.jobId)
  if (!job) {
    throw ApplicationFailure.nonRetryable('Job not found')
  }
  return job
}
