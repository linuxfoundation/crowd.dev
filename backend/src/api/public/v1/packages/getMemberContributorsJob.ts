import type { Request, Response } from 'express'
import { z } from 'zod'

import { getPackagesQx } from '@/db/packagesDb'
import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { InternalError, NotFoundError } from '@crowd/common'
import {
  AkritesExternalMemberContributors,
  InferredRepoMemberContributorsBody,
  MemberContributorJobRow,
  findAffiliatedMemberIdentities,
  findMemberContributorJob,
  findMemberContributorsPackage,
  groupAffiliatedMemberIdentities,
  toAkritesExternalMemberContributors,
} from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { WorkflowFailedError, WorkflowNotFoundError } from '@crowd/temporal'

import {
  AkritesExternalMemberContributorsJob,
  AkritesExternalMemberContributorsJobDone,
  AkritesExternalMemberContributorsJobFailed,
  RESULT_EXPIRED_REASON,
  memberContributorsJobWorkflowId,
  toDoneMemberContributorsJob,
  toFailedMemberContributorsJob,
  toPendingMemberContributorsJob,
  toUnresolvableMemberContributorsJob,
} from './memberContributorsJob'

const paramsSchema = z.object({ jobId: z.uuid() })

async function readCollectedBody(
  qx: QueryExecutor,
  purl: string,
): Promise<AkritesExternalMemberContributors> {
  const pkg = await findMemberContributorsPackage(qx, purl)
  if (!pkg || pkg.repoId === null || pkg.repoUrl === null) {
    throw new InternalError(`Done member contributors job has no linked package for ${purl}`)
  }
  const { repoId, repoUrl } = pkg
  const rows = await findAffiliatedMemberIdentities(qx, { packageId: pkg.packageId, repoId })
  return toAkritesExternalMemberContributors(
    { ...pkg, repoUrl },
    groupAffiliatedMemberIdentities(rows),
  )
}

function assertNever(value: never): never {
  throw new InternalError(`Unhandled job status ${String(value)}`)
}

async function readInferredRepoBody(
  jobId: string,
): Promise<InferredRepoMemberContributorsBody | null> {
  const packagesTemporal = await getPackagesTemporalClient()
  let body: InferredRepoMemberContributorsBody | undefined
  try {
    body = await packagesTemporal.workflow
      .getHandle<() => Promise<InferredRepoMemberContributorsBody | undefined>>(
        memberContributorsJobWorkflowId(jobId),
      )
      .result()
  } catch (err) {
    if (err instanceof WorkflowNotFoundError || err instanceof WorkflowFailedError) {
      return null
    }
    throw err
  }
  if (!body) {
    throw new InternalError(`Done member contributors job ${jobId} has no workflow result`)
  }
  return body
}

async function toDoneResponse(
  qx: QueryExecutor,
  job: MemberContributorJobRow,
): Promise<AkritesExternalMemberContributorsJobDone | AkritesExternalMemberContributorsJobFailed> {
  if (job.collectionPath !== 'llm_repo_resolution') {
    return toDoneMemberContributorsJob(job.id, await readCollectedBody(qx, job.purl))
  }
  const body = await readInferredRepoBody(job.id)
  return body
    ? toDoneMemberContributorsJob(job.id, body)
    : toFailedMemberContributorsJob(job.id, RESULT_EXPIRED_REASON)
}

async function toJobResponse(
  qx: QueryExecutor,
  job: MemberContributorJobRow,
): Promise<AkritesExternalMemberContributorsJob> {
  switch (job.status) {
    case 'done':
      return toDoneResponse(qx, job)
    case 'failed':
      return toFailedMemberContributorsJob(job.id, job.error)
    case 'unresolvable':
      return toUnresolvableMemberContributorsJob(job.id, job.error, job.repoUrl)
    case 'pending':
    case 'running':
      return toPendingMemberContributorsJob(job.id)
    default:
      return assertNever(job.status)
  }
}

export async function getMemberContributorsJob(req: Request, res: Response): Promise<void> {
  const { jobId } = validateOrThrow(paramsSchema, req.params)
  const qx = await getPackagesQx()
  const job = await findMemberContributorJob(qx, jobId)
  if (!job) {
    throw new NotFoundError()
  }
  ok(res, await toJobResponse(qx, job))
}
