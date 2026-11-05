import type { Request, Response } from 'express'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { BadRequestError, ConflictError, NotFoundError } from '@crowd/common'
import type { MemberContributorsPackageRow } from '@crowd/data-access-layer'
import { WorkflowExecutionAlreadyStartedError, WorkflowNotFoundError } from '@crowd/temporal'

import { submitMemberContributorsJob } from './submitMemberContributorsJob'

const {
  start,
  getHandle,
  describe: describeWorkflow,
  findMemberContributorsPackage,
  findActiveMemberContributorJobByPurl,
  findLatestMemberContributorJobStatusByPurl,
  findLatestMemberContributorJobByPurl,
  incrementMemberContributorJobSubmissions,
  createMemberContributorJob,
  failMemberContributorJob,
} = vi.hoisted(() => ({
  start: vi.fn(),
  getHandle: vi.fn(),
  describe: vi.fn(),
  findMemberContributorsPackage: vi.fn(),
  findActiveMemberContributorJobByPurl: vi.fn(),
  findLatestMemberContributorJobStatusByPurl: vi.fn(),
  findLatestMemberContributorJobByPurl: vi.fn(),
  incrementMemberContributorJobSubmissions: vi.fn(),
  createMemberContributorJob: vi.fn(),
  failMemberContributorJob: vi.fn(),
}))

vi.mock('@/db/packagesTemporal', () => ({
  getPackagesTemporalClient: vi.fn().mockResolvedValue({ workflow: { start, getHandle } }),
}))
vi.mock('@crowd/temporal', () => {
  class MockWorkflowExecutionAlreadyStartedError extends Error {
    name = 'WorkflowExecutionAlreadyStartedError'
  }
  class MockWorkflowNotFoundError extends Error {
    name = 'WorkflowNotFoundError'
  }
  return {
    WorkflowIdConflictPolicy: { USE_EXISTING: 'USE_EXISTING' },
    WorkflowIdReusePolicy: { REJECT_DUPLICATE: 'REJECT_DUPLICATE' },
    WorkflowExecutionAlreadyStartedError: MockWorkflowExecutionAlreadyStartedError,
    WorkflowNotFoundError: MockWorkflowNotFoundError,
  }
})
vi.mock('@/db/packagesDb', () => ({ getPackagesQx: vi.fn().mockResolvedValue({}) }))
vi.mock('@crowd/data-access-layer', () => ({
  findMemberContributorsPackage,
  findActiveMemberContributorJobByPurl,
  findLatestMemberContributorJobStatusByPurl,
  findLatestMemberContributorJobByPurl,
  incrementMemberContributorJobSubmissions,
  createMemberContributorJob,
  failMemberContributorJob,
}))

function packageRow(
  overrides: Partial<MemberContributorsPackageRow> = {},
): MemberContributorsPackageRow {
  return {
    packageId: '2008420',
    purl: 'pkg:npm/request-oauth',
    name: 'request-oauth',
    ecosystem: 'npm',
    repoId: '590455',
    repoUrl: 'https://github.com/a/request-oauth',
    repoMappingConfidence: '0.9',
    hasContributorRows: false,
    ...overrides,
  }
}

function mockReqRes(body: unknown) {
  const req = { body } as unknown as Request
  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status, json } as unknown as Response
  return { req, res, status, json }
}

function expectRedispatched(jobId: string) {
  expect(start).toHaveBeenCalledTimes(1)
  const [workflowType, options] = start.mock.calls[0]
  expect(workflowType).toBe('collectMemberContributors')
  expect(options).toMatchObject({
    taskQueue: 'security-contacts-worker',
    workflowId: `member-contributors-job/${jobId}`,
    workflowIdConflictPolicy: 'USE_EXISTING',
    workflowIdReusePolicy: 'REJECT_DUPLICATE',
    retry: { maximumAttempts: 1 },
    args: [{ jobId }],
  })
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

beforeEach(() => {
  vi.clearAllMocks()
  start.mockResolvedValue(undefined)
  getHandle.mockReturnValue({ describe: describeWorkflow })
  describeWorkflow.mockRejectedValue(new WorkflowNotFoundError('not found', 'id', undefined))
  findMemberContributorsPackage.mockResolvedValue(packageRow())
  findActiveMemberContributorJobByPurl.mockResolvedValue(null)
  findLatestMemberContributorJobStatusByPurl.mockResolvedValue(null)
  findLatestMemberContributorJobByPurl.mockResolvedValue(null)
  incrementMemberContributorJobSubmissions.mockResolvedValue(undefined)
  createMemberContributorJob.mockImplementation(async (_qx, input) => input.id)
  failMemberContributorJob.mockResolvedValue(undefined)
})

describe('submitMemberContributorsJob', () => {
  it('creates a pending job, starts the workflow on security-contacts-worker and responds 202', async () => {
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    expect(createMemberContributorJob).toHaveBeenCalledTimes(1)
    const [, input] = createMemberContributorJob.mock.calls[0]
    expect(input).toMatchObject({
      purl: 'pkg:npm/request-oauth',
      ecosystem: 'npm',
      collectionPath: 'github',
      packageId: '2008420',
      repoId: '590455',
    })
    expect(input.id).toMatch(UUID)

    expect(start).toHaveBeenCalledTimes(1)
    const [workflowType, options] = start.mock.calls[0]
    expect(workflowType).toBe('collectMemberContributors')
    expect(options).toMatchObject({
      taskQueue: 'security-contacts-worker',
      workflowId: `member-contributors-job/${input.id}`,
      workflowIdConflictPolicy: 'USE_EXISTING',
      workflowIdReusePolicy: 'REJECT_DUPLICATE',
      retry: { maximumAttempts: 1 },
      args: [{ jobId: input.id }],
    })
    expect(options).not.toHaveProperty('workflowExecutionTimeout')

    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: input.id, status: 'pending' })
  })

  it('normalizes the purl before any lookup', async () => {
    const { req, res } = mockReqRes({ purl: 'pkg:npm/@scope/pkg@1.2.3' })
    await submitMemberContributorsJob(req, res)
    expect(findMemberContributorsPackage).toHaveBeenCalledWith({}, 'pkg:npm/%40scope/pkg')
  })

  it('rejects a malformed purl with 400 before any lookup', async () => {
    const { req, res } = mockReqRes({ purl: 'npm/express' })
    const error = await submitMemberContributorsJob(req, res).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(BadRequestError)
    expect(findMemberContributorsPackage).not.toHaveBeenCalled()
  })

  it('accepts a golang purl and stores the package ecosystem', async () => {
    findMemberContributorsPackage.mockResolvedValue(
      packageRow({ purl: 'pkg:golang/github.com/x/y', ecosystem: 'go' }),
    )
    const { req, res, status } = mockReqRes({ purl: 'pkg:golang/github.com/x/y' })

    await submitMemberContributorsJob(req, res)

    const [, input] = createMemberContributorJob.mock.calls[0]
    expect(input).toMatchObject({
      purl: 'pkg:golang/github.com/x/y',
      ecosystem: 'go',
      collectionPath: 'github',
    })
    expect(status).toHaveBeenCalledWith(202)
  })

  it('rejects an unknown purl with 404', async () => {
    findMemberContributorsPackage.mockResolvedValue(null)
    const { req, res } = mockReqRes({ purl: 'pkg:npm/nope' })
    await expect(submitMemberContributorsJob(req, res)).rejects.toBeInstanceOf(NotFoundError)
    expect(createMemberContributorJob).not.toHaveBeenCalled()
  })

  it('rejects a package without a linked repository with 404', async () => {
    findMemberContributorsPackage.mockResolvedValue(
      packageRow({ repoId: null, repoUrl: null, repoMappingConfidence: null }),
    )
    const { req, res } = mockReqRes({ purl: 'pkg:npm/request-oauth' })
    const error = await submitMemberContributorsJob(req, res).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(NotFoundError)
    expect((error as NotFoundError).message).toBe('Purl has no linked repository')
    expect(createMemberContributorJob).not.toHaveBeenCalled()
  })

  it('rejects a package whose repo already has contributor rows with 409', async () => {
    findMemberContributorsPackage.mockResolvedValue(packageRow({ hasContributorRows: true }))
    const { req, res } = mockReqRes({ purl: 'pkg:npm/express' })
    const error = await submitMemberContributorsJob(req, res).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(ConflictError)
    expect((error as ConflictError).message).toBe(
      'Member contributors already collected for this purl',
    )
    expect(findLatestMemberContributorJobStatusByPurl).toHaveBeenCalledWith({}, 'pkg:npm/express')
    expect(createMemberContributorJob).not.toHaveBeenCalled()
    expect(start).not.toHaveBeenCalled()
  })

  it('accepts a collected purl whose latest job failed', async () => {
    findMemberContributorsPackage.mockResolvedValue(packageRow({ hasContributorRows: true }))
    findLatestMemberContributorJobStatusByPurl.mockResolvedValue('failed')
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/express' })

    await submitMemberContributorsJob(req, res)

    expect(createMemberContributorJob).toHaveBeenCalledTimes(1)
    const [, input] = createMemberContributorJob.mock.calls[0]
    expect(input.id).toMatch(UUID)
    expect(start).toHaveBeenCalledTimes(1)
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: input.id, status: 'pending' })
  })

  it('does not read the latest job status when the repo has no contributor rows', async () => {
    const { req, res } = mockReqRes({ purl: 'pkg:npm/request-oauth' })
    await submitMemberContributorsJob(req, res)
    expect(findLatestMemberContributorJobStatusByPurl).not.toHaveBeenCalled()
  })

  it('still refuses when the latest job is done', async () => {
    findMemberContributorsPackage.mockResolvedValue(packageRow({ hasContributorRows: true }))
    findLatestMemberContributorJobStatusByPurl.mockResolvedValue('done')
    const { req, res } = mockReqRes({ purl: 'pkg:npm/express' })
    const error = await submitMemberContributorsJob(req, res).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(ConflictError)
    expect(createMemberContributorJob).not.toHaveBeenCalled()
    expect(start).not.toHaveBeenCalled()
  })

  it('returns the active job instead of 409 while a job for a collected repo is still running', async () => {
    findMemberContributorsPackage.mockResolvedValue(packageRow({ hasContributorRows: true }))
    findActiveMemberContributorJobByPurl.mockResolvedValue({ id: 'running-job' })
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    expect(createMemberContributorJob).not.toHaveBeenCalled()
    expectRedispatched('running-job')
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: 'running-job', status: 'pending' })
  })

  it('returns the existing job id when one is still active for the purl', async () => {
    findActiveMemberContributorJobByPurl.mockResolvedValue({ id: 'existing-job' })
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    expect(createMemberContributorJob).not.toHaveBeenCalled()
    expect(incrementMemberContributorJobSubmissions).toHaveBeenCalledWith({}, 'existing-job')
    expectRedispatched('existing-job')
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: 'existing-job', status: 'pending' })
  })

  it('ignores an already closed workflow when redispatching an active job', async () => {
    findActiveMemberContributorJobByPurl.mockResolvedValue({ id: 'existing-job' })
    start.mockRejectedValue(new WorkflowExecutionAlreadyStartedError('closed', 'id', 'type'))
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    expect(failMemberContributorJob).not.toHaveBeenCalled()
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: 'existing-job', status: 'pending' })
  })

  it('propagates other redispatch errors without failing the existing job', async () => {
    findActiveMemberContributorJobByPurl.mockResolvedValue({ id: 'existing-job' })
    start.mockRejectedValue(new Error('temporal unreachable'))
    const { req, res, status } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await expect(submitMemberContributorsJob(req, res)).rejects.toThrow('temporal unreachable')

    expect(failMemberContributorJob).not.toHaveBeenCalled()
    expect(createMemberContributorJob).not.toHaveBeenCalled()
    expect(status).not.toHaveBeenCalled()
  })

  it('returns the winner when a concurrent submission beat the insert', async () => {
    createMemberContributorJob.mockResolvedValue(null)
    findActiveMemberContributorJobByPurl
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ id: 'winner-job' })
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    expectRedispatched('winner-job')
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: 'winner-job', status: 'pending' })
  })

  it('marks the job failed and rethrows when the workflow start fails', async () => {
    start.mockRejectedValue(new Error('temporal unreachable'))
    const { req, res, status } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await expect(submitMemberContributorsJob(req, res)).rejects.toThrow('temporal unreachable')

    const jobId = createMemberContributorJob.mock.calls[0][1].id
    expect(getHandle).toHaveBeenCalledWith(`member-contributors-job/${jobId}`)
    expect(failMemberContributorJob).toHaveBeenCalledWith({}, jobId, 'temporal unreachable')
    expect(status).not.toHaveBeenCalled()
  })

  it('responds 202 without failing the job when the start error was a lost response and the workflow exists', async () => {
    start.mockRejectedValue(new Error('DEADLINE_EXCEEDED'))
    describeWorkflow.mockResolvedValue({})
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    const jobId = createMemberContributorJob.mock.calls[0][1].id
    expect(getHandle).toHaveBeenCalledWith(`member-contributors-job/${jobId}`)
    expect(failMemberContributorJob).not.toHaveBeenCalled()
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId, status: 'pending' })
  })

  it('responds 202 without failing the job when the workflow was already started by a concurrent submission', async () => {
    start.mockRejectedValueOnce(new WorkflowExecutionAlreadyStartedError('started', 'id', 'type'))
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await submitMemberContributorsJob(req, res)

    const jobId = createMemberContributorJob.mock.calls[0][1].id
    expect(failMemberContributorJob).not.toHaveBeenCalled()
    expect(getHandle).not.toHaveBeenCalled()
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId, status: 'pending' })
  })

  it('marks the job failed and rethrows when the Temporal client cannot be obtained', async () => {
    vi.mocked(getPackagesTemporalClient).mockRejectedValueOnce(
      new Error('packages temporal not configured'),
    )
    const { req, res, status } = mockReqRes({ purl: 'pkg:npm/request-oauth' })

    await expect(submitMemberContributorsJob(req, res)).rejects.toThrow(
      'packages temporal not configured',
    )

    const jobId = createMemberContributorJob.mock.calls[0][1].id
    expect(failMemberContributorJob).toHaveBeenCalledWith(
      {},
      jobId,
      'packages temporal not configured',
    )
    expect(start).not.toHaveBeenCalled()
    expect(getHandle).not.toHaveBeenCalled()
    expect(status).not.toHaveBeenCalled()
  })
})

describe('submitMemberContributorsJob, unknown ecosystem', () => {
  const INFERRED_WORKFLOW = 'collectMemberContributorsFromInferredRepo'

  function inferredStart(jobId: string) {
    return [
      INFERRED_WORKFLOW,
      {
        taskQueue: 'security-contacts-worker',
        workflowId: `member-contributors-job/${jobId}`,
        workflowIdConflictPolicy: 'USE_EXISTING',
        workflowIdReusePolicy: 'REJECT_DUPLICATE',
        retry: { maximumAttempts: 1 },
        args: [{ jobId }],
      },
    ]
  }

  function latestJob(id: string, latestStatus: 'done' | 'failed' | 'unresolvable') {
    findLatestMemberContributorJobByPurl.mockResolvedValue({
      id,
      status: latestStatus,
      collectionPath: 'llm_repo_resolution',
    })
  }

  it.each(['pkg:hackage/aeson', 'pkg:Hackage/aeson'])(
    'creates an llm repo resolution job for %s without a package lookup and starts the inferred-repo workflow',
    async (requestedPurl) => {
      const purl = 'pkg:hackage/aeson'
      const { req, res, status, json } = mockReqRes({ purl: requestedPurl })

      await submitMemberContributorsJob(req, res)

      expect(findActiveMemberContributorJobByPurl.mock.calls).toEqual([[{}, purl]])
      expect(findLatestMemberContributorJobByPurl.mock.calls).toEqual([[{}, purl]])

      const [, input] = createMemberContributorJob.mock.calls[0]
      expect(createMemberContributorJob.mock.calls).toEqual([
        [
          {},
          {
            id: expect.stringMatching(UUID),
            purl,
            ecosystem: 'hackage',
            collectionPath: 'llm_repo_resolution',
            packageId: null,
            repoId: null,
          },
        ],
      ])
      expect(start.mock.calls).toEqual([inferredStart(input.id)])
      expect(findMemberContributorsPackage).not.toHaveBeenCalled()
      expect(status).toHaveBeenCalledWith(202)
      expect(json).toHaveBeenCalledWith({ jobId: input.id, status: 'pending' })
    },
  )

  it.each(['pkg:', 'pkg:hackage', 'pkg:/x', 'pkg:hackage/aeson%ZZ'])(
    'rejects the malformed purl %s with 400 before any database call',
    async (purl) => {
      const { req, res } = mockReqRes({ purl })
      const error = await submitMemberContributorsJob(req, res).catch((e: unknown) => e)
      expect(error).toBeInstanceOf(BadRequestError)
      expect((error as BadRequestError).message).toBe('Malformed purl')
      expect(
        [
          findActiveMemberContributorJobByPurl,
          findLatestMemberContributorJobByPurl,
          createMemberContributorJob,
          start,
        ].map((fn) => fn.mock.calls.length),
      ).toEqual([0, 0, 0, 0])
    },
  )

  it('returns the active job, counts the submission and re-dispatches the inferred-repo workflow', async () => {
    findActiveMemberContributorJobByPurl.mockResolvedValue({ id: 'active-1' })
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:hackage/aeson' })

    await submitMemberContributorsJob(req, res)

    expect(incrementMemberContributorJobSubmissions.mock.calls).toEqual([[{}, 'active-1']])
    expect(start.mock.calls).toEqual([inferredStart('active-1')])
    expect(createMemberContributorJob).not.toHaveBeenCalled()
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith({ jobId: 'active-1', status: 'pending' })
  })

  it.each(['COMPLETED', 'RUNNING'])(
    'refuses with 409 carrying the previous done jobId while its workflow is %s',
    async (workflowStatus) => {
      latestJob('done-1', 'done')
      describeWorkflow.mockResolvedValue({ status: { name: workflowStatus } })
      const { req, res } = mockReqRes({ purl: 'pkg:hackage/aeson' })

      const error = await submitMemberContributorsJob(req, res).catch((e: unknown) => e)

      expect(error).toBeInstanceOf(ConflictError)
      expect((error as ConflictError).context).toEqual({ jobId: 'done-1' })
      expect(getHandle.mock.calls).toEqual([['member-contributors-job/done-1']])
      expect(incrementMemberContributorJobSubmissions.mock.calls).toEqual([[{}, 'done-1']])
      expect(createMemberContributorJob).not.toHaveBeenCalled()
    },
  )

  it.each([
    {
      latestStatus: 'done' as const,
      workflowStatus: 'not found',
      describedWorkflows: [['member-contributors-job/old-1']],
    },
    {
      latestStatus: 'done' as const,
      workflowStatus: 'FAILED',
      describedWorkflows: [['member-contributors-job/old-1']],
    },
    { latestStatus: 'failed' as const, workflowStatus: 'not found', describedWorkflows: [] },
    { latestStatus: 'unresolvable' as const, workflowStatus: 'not found', describedWorkflows: [] },
  ])(
    'accepts a new job when the latest job is $latestStatus with its workflow $workflowStatus',
    async ({ latestStatus, workflowStatus, describedWorkflows }) => {
      latestJob('old-1', latestStatus)
      if (workflowStatus !== 'not found') {
        describeWorkflow.mockResolvedValue({ status: { name: workflowStatus } })
      }
      const { req, res, status } = mockReqRes({ purl: 'pkg:hackage/aeson' })

      await submitMemberContributorsJob(req, res)

      expect({
        describedWorkflows: getHandle.mock.calls,
        created: createMemberContributorJob.mock.calls.length,
        submissionsCounted: incrementMemberContributorJobSubmissions.mock.calls,
        statuses: status.mock.calls,
      }).toEqual({ describedWorkflows, created: 1, submissionsCounted: [], statuses: [[202]] })
    },
  )
})
