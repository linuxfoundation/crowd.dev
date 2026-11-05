import type { Request, Response } from 'express'

import { getPackagesQx } from '@/db/packagesDb'
import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { validateOrThrow } from '@/utils/validation'
import { BadRequestError, ConflictError, NotFoundError, generateUUIDv4 } from '@crowd/common'
import {
  MemberContributorCollectionPath,
  MemberContributorsPackageRow,
  createMemberContributorJob,
  failMemberContributorJob,
  findActiveMemberContributorJobByPurl,
  findLatestMemberContributorJobByPurl,
  findLatestMemberContributorJobStatusByPurl,
  findMemberContributorsPackage,
  incrementMemberContributorJobSubmissions,
} from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  type Client,
  WorkflowExecutionAlreadyStartedError,
  WorkflowIdConflictPolicy,
  WorkflowIdReusePolicy,
  WorkflowNotFoundError,
} from '@crowd/temporal'

import {
  LLM_REPO_RESOLUTION_PATH,
  canonicalizePurlType,
  collectionPathForPurlType,
  isWellFormedPurl,
  purlType,
} from './memberContributorCollectionPaths'
import {
  MEMBER_CONTRIBUTORS_INFERRED_REPO_WORKFLOW,
  MEMBER_CONTRIBUTORS_JOB_TASK_QUEUE,
  MEMBER_CONTRIBUTORS_JOB_WORKFLOW,
  memberContributorsJobWorkflowId,
  toPendingMemberContributorsJob,
} from './memberContributorsJob'
import { purlBodySchema } from './purl'

type LinkedPackage = MemberContributorsPackageRow & { repoId: string }

function resolveCollectionPath(purl: string): {
  purlType: string
  collectionPath: MemberContributorCollectionPath
} {
  const type = purlType(purl)
  return { purlType: type, collectionPath: collectionPathForPurlType(type) }
}

async function findLinkedPackage(qx: QueryExecutor, purl: string): Promise<LinkedPackage> {
  const pkg = await findMemberContributorsPackage(qx, purl)
  if (!pkg) {
    throw new NotFoundError()
  }
  if (pkg.repoId === null) {
    throw new NotFoundError('Purl has no linked repository')
  }
  return { ...pkg, repoId: pkg.repoId }
}

async function assertCollectable(
  qx: QueryExecutor,
  pkg: LinkedPackage,
  purl: string,
): Promise<void> {
  if (!pkg.hasContributorRows) {
    return
  }
  const latestStatus = await findLatestMemberContributorJobStatusByPurl(qx, purl)
  if (latestStatus !== 'failed') {
    throw new ConflictError('Member contributors already collected for this purl')
  }
}

async function findActiveJobIdOrThrow(qx: QueryExecutor, purl: string): Promise<string> {
  const active = await findActiveMemberContributorJobByPurl(qx, purl)
  if (!active) {
    throw new ConflictError('Member contributors job insert raced and no active job was found')
  }
  return active.id
}

function jobWorkflowStartOptions(jobId: string) {
  return {
    taskQueue: MEMBER_CONTRIBUTORS_JOB_TASK_QUEUE,
    workflowId: memberContributorsJobWorkflowId(jobId),
    workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
    workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
    retry: { maximumAttempts: 1 },
    args: [{ jobId }],
  }
}

async function ensureJobWorkflowStarted(jobId: string, workflowType: string): Promise<void> {
  const packagesTemporal = await getPackagesTemporalClient()
  try {
    await packagesTemporal.workflow.start(workflowType, jobWorkflowStartOptions(jobId))
  } catch (err) {
    if (!(err instanceof WorkflowExecutionAlreadyStartedError)) {
      throw err
    }
  }
}

async function workflowExists(packagesTemporal: Client, jobId: string): Promise<boolean> {
  try {
    await packagesTemporal.workflow.getHandle(memberContributorsJobWorkflowId(jobId)).describe()
    return true
  } catch {
    return false
  }
}

async function startJobWorkflow(
  qx: QueryExecutor,
  jobId: string,
  workflowType: string,
): Promise<void> {
  let packagesTemporal: Client | undefined
  try {
    packagesTemporal = await getPackagesTemporalClient()
    await packagesTemporal.workflow.start(workflowType, jobWorkflowStartOptions(jobId))
  } catch (err) {
    if (err instanceof WorkflowExecutionAlreadyStartedError) {
      return
    }
    if (packagesTemporal && (await workflowExists(packagesTemporal, jobId))) {
      return
    }
    const message = err instanceof Error ? err.message : String(err)
    await failMemberContributorJob(qx, jobId, message)
    throw err
  }
}

const READABLE_DONE_JOB_WORKFLOW_STATUSES = new Set(['COMPLETED', 'RUNNING'])

async function doneJobWorkflowResultReadable(jobId: string): Promise<boolean> {
  const packagesTemporal = await getPackagesTemporalClient()
  try {
    const description = await packagesTemporal.workflow
      .getHandle(memberContributorsJobWorkflowId(jobId))
      .describe()
    return READABLE_DONE_JOB_WORKFLOW_STATUSES.has(description.status.name)
  } catch (err) {
    if (err instanceof WorkflowNotFoundError) {
      return false
    }
    throw err
  }
}

async function assertInferredRepoResultNotReadable(qx: QueryExecutor, purl: string): Promise<void> {
  const latest = await findLatestMemberContributorJobByPurl(qx, purl)
  if (!latest || latest.status !== 'done' || latest.collectionPath !== LLM_REPO_RESOLUTION_PATH) {
    return
  }
  if (!(await doneJobWorkflowResultReadable(latest.id))) {
    return
  }
  await incrementMemberContributorJobSubmissions(qx, latest.id)
  throw new ConflictError('Member contributors already collected for this purl', {
    jobId: latest.id,
  })
}

async function respondWithActiveJob(
  qx: QueryExecutor,
  jobId: string,
  workflowType: string,
  res: Response,
): Promise<void> {
  await incrementMemberContributorJobSubmissions(qx, jobId)
  await ensureJobWorkflowStarted(jobId, workflowType)
  res.status(202).json(toPendingMemberContributorsJob(jobId))
}

async function submitInferredRepoJob(
  qx: QueryExecutor,
  requestedPurl: string,
  ecosystem: string,
  res: Response,
): Promise<void> {
  if (!isWellFormedPurl(requestedPurl)) {
    throw new BadRequestError('Malformed purl')
  }
  const purl = canonicalizePurlType(requestedPurl)
  const active = await findActiveMemberContributorJobByPurl(qx, purl)
  if (active) {
    await respondWithActiveJob(qx, active.id, MEMBER_CONTRIBUTORS_INFERRED_REPO_WORKFLOW, res)
    return
  }
  await assertInferredRepoResultNotReadable(qx, purl)
  const created = await createMemberContributorJob(qx, {
    id: generateUUIDv4(),
    purl,
    ecosystem,
    collectionPath: LLM_REPO_RESOLUTION_PATH,
    packageId: null,
    repoId: null,
  })
  if (created === null) {
    await respondWithActiveJob(
      qx,
      await findActiveJobIdOrThrow(qx, purl),
      MEMBER_CONTRIBUTORS_INFERRED_REPO_WORKFLOW,
      res,
    )
    return
  }
  await startJobWorkflow(qx, created, MEMBER_CONTRIBUTORS_INFERRED_REPO_WORKFLOW)
  res.status(202).json(toPendingMemberContributorsJob(created))
}

export async function submitMemberContributorsJob(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlBodySchema, req.body)
  const { purlType: type, collectionPath } = resolveCollectionPath(purl)

  const qx = await getPackagesQx()
  if (collectionPath === LLM_REPO_RESOLUTION_PATH) {
    await submitInferredRepoJob(qx, purl, type, res)
    return
  }

  const pkg = await findLinkedPackage(qx, purl)

  const active = await findActiveMemberContributorJobByPurl(qx, purl)
  if (active) {
    await respondWithActiveJob(qx, active.id, MEMBER_CONTRIBUTORS_JOB_WORKFLOW, res)
    return
  }

  await assertCollectable(qx, pkg, purl)

  const created = await createMemberContributorJob(qx, {
    id: generateUUIDv4(),
    purl,
    ecosystem: pkg.ecosystem,
    collectionPath,
    packageId: pkg.packageId,
    repoId: pkg.repoId,
  })
  if (created === null) {
    await respondWithActiveJob(
      qx,
      await findActiveJobIdOrThrow(qx, purl),
      MEMBER_CONTRIBUTORS_JOB_WORKFLOW,
      res,
    )
    return
  }

  await startJobWorkflow(qx, created, MEMBER_CONTRIBUTORS_JOB_WORKFLOW)
  res.status(202).json(toPendingMemberContributorsJob(created))
}
