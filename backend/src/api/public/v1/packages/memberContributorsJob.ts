import type {
  AkritesExternalMemberContributors,
  InferredRepoMemberContributorsBody,
} from '@crowd/data-access-layer'
import { TemporalWorkflowId } from '@crowd/types'

export const MEMBER_CONTRIBUTORS_JOB_TASK_QUEUE = 'security-contacts-worker'
export const MEMBER_CONTRIBUTORS_JOB_WORKFLOW = 'collectMemberContributors'
export const MEMBER_CONTRIBUTORS_INFERRED_REPO_WORKFLOW =
  'collectMemberContributorsFromInferredRepo'
export const RESULT_EXPIRED_REASON = 'Job result expired, resubmit the job'

export interface AkritesExternalMemberContributorsJobPending {
  jobId: string
  status: 'pending'
}

export interface AkritesExternalMemberContributorsJobFailed {
  jobId: string
  status: 'failed'
  reason: string
}

export interface AkritesExternalMemberContributorsJobUnresolvable {
  jobId: string
  status: 'unresolvable'
  reason: string
  repoUrl: string | null
}

export type MemberContributorsDoneBody =
  | Omit<AkritesExternalMemberContributors, 'status'>
  | InferredRepoMemberContributorsBody

export type AkritesExternalMemberContributorsJobDone = MemberContributorsDoneBody & {
  jobId: string
  status: 'done'
}

export type AkritesExternalMemberContributorsJob =
  | AkritesExternalMemberContributorsJobPending
  | AkritesExternalMemberContributorsJobFailed
  | AkritesExternalMemberContributorsJobUnresolvable
  | AkritesExternalMemberContributorsJobDone

export function memberContributorsJobWorkflowId(jobId: string): string {
  return `${TemporalWorkflowId.MEMBER_CONTRIBUTORS_JOB}/${jobId}`
}

export function toPendingMemberContributorsJob(
  jobId: string,
): AkritesExternalMemberContributorsJobPending {
  return { jobId, status: 'pending' }
}

export function toFailedMemberContributorsJob(
  jobId: string,
  reason: string | null,
): AkritesExternalMemberContributorsJobFailed {
  return { jobId, status: 'failed', reason: reason ?? 'Job failed' }
}

export function toUnresolvableMemberContributorsJob(
  jobId: string,
  reason: string | null,
  repoUrl: string | null,
): AkritesExternalMemberContributorsJobUnresolvable {
  return {
    jobId,
    status: 'unresolvable',
    reason: reason ?? 'Repository could not be resolved',
    repoUrl,
  }
}

export function toDoneMemberContributorsJob(
  jobId: string,
  body: MemberContributorsDoneBody,
): AkritesExternalMemberContributorsJobDone {
  return { ...body, jobId, status: 'done' }
}
