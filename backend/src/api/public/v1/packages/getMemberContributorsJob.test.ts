import type { Request, Response } from 'express'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError, InternalError, NotFoundError } from '@crowd/common'
import type {
  AffiliatedMemberIdentityRow,
  MemberContributorJobRow,
  MemberContributorsPackageRow,
} from '@crowd/data-access-layer'
import { canonicalCdpOrganizationIdByMemberName } from '@crowd/data-access-layer/src/osspckgs/akritesMembers'
import { WorkflowFailedError, WorkflowNotFoundError } from '@crowd/temporal'

import { getMemberContributorsJob } from './getMemberContributorsJob'

const {
  getHandle,
  result,
  findMemberContributorJob,
  findMemberContributorsPackage,
  findAffiliatedMemberIdentities,
} = vi.hoisted(() => ({
  getHandle: vi.fn(),
  result: vi.fn(),
  findMemberContributorJob: vi.fn(),
  findMemberContributorsPackage: vi.fn(),
  findAffiliatedMemberIdentities: vi.fn(),
}))

vi.mock('@/db/packagesTemporal', () => ({
  getPackagesTemporalClient: vi.fn().mockResolvedValue({ workflow: { getHandle } }),
}))
vi.mock('@crowd/temporal', () => {
  class MockWorkflowNotFoundError extends Error {
    name = 'WorkflowNotFoundError'
  }
  class MockWorkflowFailedError extends Error {
    name = 'WorkflowFailedError'
  }
  return {
    WorkflowNotFoundError: MockWorkflowNotFoundError,
    WorkflowFailedError: MockWorkflowFailedError,
  }
})
vi.mock('@/db/packagesDb', () => ({ getPackagesQx: vi.fn().mockResolvedValue({}) }))
vi.mock('@crowd/data-access-layer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/data-access-layer')>()),
  findMemberContributorJob,
  findMemberContributorsPackage,
  findAffiliatedMemberIdentities,
}))

const JOB_ID = '11111111-1111-4111-8111-111111111111'

function jobRow(overrides: Partial<MemberContributorJobRow> = {}): MemberContributorJobRow {
  return {
    id: JOB_ID,
    purl: 'pkg:npm/request-oauth',
    ecosystem: 'npm',
    collectionPath: 'github',
    status: 'pending',
    error: null,
    packageId: '2008420',
    repoId: '590455',
    repoUrl: null,
    repoUrlSource: null,
    ...overrides,
  }
}

function packageRow(): MemberContributorsPackageRow {
  return {
    packageId: '2008420',
    purl: 'pkg:npm/request-oauth',
    name: 'request-oauth',
    ecosystem: 'npm',
    repoId: '590455',
    repoUrl: 'https://github.com/a/request-oauth',
    repoMappingConfidence: '0.9',
    hasContributorRows: true,
  }
}

function identityRow(): AffiliatedMemberIdentityRow {
  return {
    person: 'person-1',
    source: 'github',
    role: 'contributor',
    roleKind: 'contributor',
    identityType: 'github-login',
    identityValue: 'octo',
    endedAt: null,
    lastSeenAt: '2026-10-15T00:00:00.000Z',
    registryUsername: null,
    registryDisplayName: null,
    akritesMember: 'Google',
    cdpOrganizationId: 'org-google',
    confidence: 'cdp',
    confidenceScore: '1.0',
  }
}

function mockReqRes(params: Record<string, unknown>) {
  const req = { params } as unknown as Request
  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status, json } as unknown as Response
  return { req, res, status, json }
}

beforeEach(() => {
  vi.clearAllMocks()
  getHandle.mockReturnValue({ result })
})

describe('getMemberContributorsJob', () => {
  it('rejects a non-uuid job id with 400 before any lookup', async () => {
    const { req, res } = mockReqRes({ jobId: 'nope' })
    await expect(getMemberContributorsJob(req, res)).rejects.toBeInstanceOf(BadRequestError)
    expect(findMemberContributorJob).not.toHaveBeenCalled()
  })

  it('responds 404 for an unknown job', async () => {
    findMemberContributorJob.mockResolvedValue(null)
    const { req, res } = mockReqRes({ jobId: JOB_ID })
    await expect(getMemberContributorsJob(req, res)).rejects.toBeInstanceOf(NotFoundError)
  })

  it.each(['pending', 'running'] as const)('reports %s as pending', async (status) => {
    findMemberContributorJob.mockResolvedValue(jobRow({ status }))
    const { req, res, json } = mockReqRes({ jobId: JOB_ID })
    await getMemberContributorsJob(req, res)
    expect(json).toHaveBeenCalledWith({ jobId: JOB_ID, status: 'pending' })
    expect(findMemberContributorsPackage).not.toHaveBeenCalled()
  })

  it('reports failed with the stored reason', async () => {
    findMemberContributorJob.mockResolvedValue(
      jobRow({ status: 'failed', error: 'Repository not found on GitHub' }),
    )
    const { req, res, json } = mockReqRes({ jobId: JOB_ID })
    await getMemberContributorsJob(req, res)
    expect(json).toHaveBeenCalledWith({
      jobId: JOB_ID,
      status: 'failed',
      reason: 'Repository not found on GitHub',
    })
  })

  it('reports failed with a generic reason when none was stored', async () => {
    findMemberContributorJob.mockResolvedValue(jobRow({ status: 'failed', error: null }))
    const { req, res, json } = mockReqRes({ jobId: JOB_ID })
    await getMemberContributorsJob(req, res)
    expect(json).toHaveBeenCalledWith({ jobId: JOB_ID, status: 'failed', reason: 'Job failed' })
  })

  it('returns the sync endpoint body with the job fields when done', async () => {
    findMemberContributorJob.mockResolvedValue(jobRow({ status: 'done' }))
    findMemberContributorsPackage.mockResolvedValue(packageRow())
    findAffiliatedMemberIdentities.mockResolvedValue([identityRow()])
    const { req, res, json } = mockReqRes({ jobId: JOB_ID })

    await getMemberContributorsJob(req, res)

    expect(findMemberContributorsPackage).toHaveBeenCalledWith({}, 'pkg:npm/request-oauth')
    expect(findAffiliatedMemberIdentities).toHaveBeenCalledWith(
      {},
      { packageId: '2008420', repoId: '590455' },
    )
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        jobId: JOB_ID,
        status: 'done',
        purl: 'pkg:npm/request-oauth',
        repoUrl: 'https://github.com/a/request-oauth',
        contributors: [
          expect.objectContaining({
            githubLogin: 'octo',
            organization: expect.objectContaining({
              id: canonicalCdpOrganizationIdByMemberName().get('Google'),
              name: 'Google',
            }),
          }),
        ],
      }),
    )
  })

  it('fails loudly when a done job no longer resolves to a linked package', async () => {
    findMemberContributorJob.mockResolvedValue(jobRow({ status: 'done' }))
    findMemberContributorsPackage.mockResolvedValue(null)
    const { req, res } = mockReqRes({ jobId: JOB_ID })
    await expect(getMemberContributorsJob(req, res)).rejects.toBeInstanceOf(InternalError)
  })
})

describe('getMemberContributorsJob, llm repo resolution path', () => {
  const llmJob = (overrides: Partial<MemberContributorJobRow>) =>
    jobRow({ collectionPath: 'llm_repo_resolution', ...overrides })
  const workflowId = `member-contributors-job/${JOB_ID}`
  const inferredBody = {
    purl: 'pkg:hackage/aeson',
    name: 'aeson',
    ecosystem: 'hackage',
    repoUrl: 'https://github.com/haskell/aeson',
    repoUrlSource: 'llm_inferred',
    repoMappingConfidence: null,
    contributors: [],
    maintainers: { registry: [], governanceFiles: [] },
  }

  it.each([
    {
      name: 'reports unresolvable with the reason and attempted repo url, without the workflow',
      arrange: () =>
        findMemberContributorJob.mockResolvedValue(
          llmJob({
            status: 'unresolvable',
            error: 'LLM confidence 0.4 below threshold 0.7',
            repoUrl: null,
          }),
        ),
      response: {
        jobId: JOB_ID,
        status: 'unresolvable',
        reason: 'LLM confidence 0.4 below threshold 0.7',
        repoUrl: null,
      },
      workflowIds: [],
      packageReads: [],
    },
    {
      name: 'returns the workflow result as the done body',
      arrange: () => {
        findMemberContributorJob.mockResolvedValue(llmJob({ status: 'done' }))
        result.mockResolvedValue(inferredBody)
      },
      response: { ...inferredBody, jobId: JOB_ID, status: 'done' },
      workflowIds: [[workflowId]],
      packageReads: [],
    },
    {
      name: 'reports failed with the resubmit reason when the workflow result is gone',
      arrange: () => {
        findMemberContributorJob.mockResolvedValue(llmJob({ status: 'done' }))
        result.mockRejectedValue(new WorkflowNotFoundError('gone', 'id', undefined))
      },
      response: { jobId: JOB_ID, status: 'failed', reason: 'Job result expired, resubmit the job' },
      workflowIds: [[workflowId]],
      packageReads: [],
    },
    {
      name: 'reports failed with the resubmit reason when the workflow ended abnormally',
      arrange: () => {
        findMemberContributorJob.mockResolvedValue(llmJob({ status: 'done' }))
        result.mockRejectedValue(
          new WorkflowFailedError('terminated', undefined, 'NON_RETRYABLE_FAILURE'),
        )
      },
      response: { jobId: JOB_ID, status: 'failed', reason: 'Job result expired, resubmit the job' },
      workflowIds: [[workflowId]],
      packageReads: [],
    },
    {
      name: 'keeps reading github path done jobs from the tables, not the workflow',
      arrange: () => {
        findMemberContributorJob.mockResolvedValue(jobRow({ status: 'done' }))
        findMemberContributorsPackage.mockResolvedValue(packageRow())
        findAffiliatedMemberIdentities.mockResolvedValue([identityRow()])
      },
      response: expect.objectContaining({
        jobId: JOB_ID,
        status: 'done',
        purl: 'pkg:npm/request-oauth',
      }),
      workflowIds: [],
      packageReads: [[{}, 'pkg:npm/request-oauth']],
    },
  ])('$name', async ({ arrange, response, workflowIds, packageReads }) => {
    arrange()
    const { req, res, json } = mockReqRes({ jobId: JOB_ID })
    await getMemberContributorsJob(req, res)
    expect({
      responses: json.mock.calls,
      workflowIds: getHandle.mock.calls,
      packageReads: findMemberContributorsPackage.mock.calls,
    }).toEqual({ responses: [[response]], workflowIds, packageReads })
  })

  it('throws an internal error when a done workflow returned no body', async () => {
    findMemberContributorJob.mockResolvedValue(llmJob({ status: 'done' }))
    result.mockResolvedValue(undefined)
    const { req, res } = mockReqRes({ jobId: JOB_ID })
    await expect(getMemberContributorsJob(req, res)).rejects.toBeInstanceOf(InternalError)
  })
})
