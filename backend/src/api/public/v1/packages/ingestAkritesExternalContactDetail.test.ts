import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import type { AkritesExternalContactDetailRow } from '@crowd/data-access-layer'
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from '@crowd/temporal'

import { ingestAkritesExternalContactDetail } from './ingestAkritesExternalContactDetail'

const { execute, getContactDetailsByPurls } = vi.hoisted(() => ({
  execute: vi.fn(),
  getContactDetailsByPurls: vi.fn(),
}))

vi.mock('@/db/packagesTemporal', () => ({
  getPackagesTemporalClient: vi.fn().mockResolvedValue({ workflow: { execute } }),
}))

vi.mock('@/db/packagesDb', () => ({
  getPackagesQx: vi.fn().mockResolvedValue({}),
}))

vi.mock('@crowd/data-access-layer', () => ({
  getContactDetailsByPurls,
}))

function baseRow(
  overrides: Partial<AkritesExternalContactDetailRow> = {},
): AkritesExternalContactDetailRow {
  return {
    purl: 'pkg:npm/lodash',
    name: 'lodash',
    ecosystem: 'npm',
    securityPolicyUrl: null,
    vulnerabilityReportingUrl: null,
    bugBountyUrl: null,
    pvrEnabled: null,
    declaredRepositoryUrl: null,
    resolvedRepositoryUrl: null,
    repoMappingConfidence: null,
    contactsLastRefreshed: null,
    securityContacts: [],
    ...overrides,
  }
}

function mockReqRes(body: unknown) {
  execute.mockClear()
  getContactDetailsByPurls.mockClear()

  const req = { body } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status, json } as unknown as Response

  return { req, res, status, json }
}

describe('ingestAkritesExternalContactDetail', () => {
  it('returns the existing contact detail without triggering the workflow when already ingested', async () => {
    getContactDetailsByPurls.mockResolvedValue([
      baseRow({ contactsLastRefreshed: '2024-01-01T00:00:00.000Z' }),
    ])

    const { req, res, json } = mockReqRes({ purl: 'pkg:npm/lodash' })

    await ingestAkritesExternalContactDetail(req, res)

    expect(execute).not.toHaveBeenCalled()
    expect(getContactDetailsByPurls).toHaveBeenCalledTimes(1)
    expect(json).toHaveBeenCalledWith(expect.objectContaining({ purl: 'pkg:npm/lodash' }))
  })

  it('executes ingestSecurityContactsForPurlWorkflow and returns the re-read contact detail when never ingested', async () => {
    execute.mockResolvedValue({ found: true, repoId: 'repo-1' })
    getContactDetailsByPurls
      .mockResolvedValueOnce([
        baseRow({
          resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
          contactsLastRefreshed: null,
        }),
      ])
      .mockResolvedValueOnce([baseRow({ contactsLastRefreshed: '2024-01-01T00:00:00.000Z' })])

    const { req, res, json } = mockReqRes({ purl: 'pkg:npm/lodash' })

    await ingestAkritesExternalContactDetail(req, res)

    expect(execute).toHaveBeenCalledTimes(1)
    const [workflowType, options] = execute.mock.calls[0]
    expect(workflowType).toBe('ingestSecurityContactsForPurlWorkflow')
    expect(options.taskQueue).toBe('security-contacts-worker')
    expect(options.workflowId).toMatch(/^security-contacts-ondemand:[0-9a-f]{64}$/)
    expect(options.workflowIdConflictPolicy).toBe(WorkflowIdConflictPolicy.USE_EXISTING)
    expect(options.workflowIdReusePolicy).toBe(WorkflowIdReusePolicy.ALLOW_DUPLICATE)
    expect(options.args).toEqual(['pkg:npm/lodash'])

    expect(getContactDetailsByPurls).toHaveBeenCalledTimes(2)
    expect(getContactDetailsByPurls).toHaveBeenCalledWith(expect.anything(), ['pkg:npm/lodash'])
    expect(json).toHaveBeenCalledWith(expect.objectContaining({ purl: 'pkg:npm/lodash' }))
  })

  it('derives the same deterministic workflowId for the same purl', async () => {
    execute.mockResolvedValue({ found: true })
    getContactDetailsByPurls.mockResolvedValue([
      baseRow({
        resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
        contactsLastRefreshed: null,
      }),
    ])

    const { req: req1, res: res1 } = mockReqRes({ purl: 'pkg:npm/lodash' })
    await ingestAkritesExternalContactDetail(req1, res1)
    const id1 = execute.mock.calls[0][1].workflowId

    const { req: req2, res: res2 } = mockReqRes({ purl: 'pkg:npm/lodash' })
    await ingestAkritesExternalContactDetail(req2, res2)
    const id2 = execute.mock.calls[0][1].workflowId

    expect(id1).toBe(id2)
  })

  it('throws NotFoundError without executing the workflow when the purl is unknown', async () => {
    getContactDetailsByPurls.mockResolvedValue([])

    const { req, res } = mockReqRes({ purl: 'pkg:npm/left-pad' })

    await expect(ingestAkritesExternalContactDetail(req, res)).rejects.toThrow()
    expect(execute).not.toHaveBeenCalled()
    expect(getContactDetailsByPurls).toHaveBeenCalledTimes(1)
  })

  it('throws NotFoundError without executing the workflow when the package has no linked repo', async () => {
    getContactDetailsByPurls.mockResolvedValue([
      baseRow({ resolvedRepositoryUrl: null, contactsLastRefreshed: null }),
    ])

    const { req, res } = mockReqRes({ purl: 'pkg:npm/left-pad' })

    await expect(ingestAkritesExternalContactDetail(req, res)).rejects.toThrow()
    expect(execute).not.toHaveBeenCalled()
    expect(getContactDetailsByPurls).toHaveBeenCalledTimes(1)
  })

  it('throws NotFoundError when the workflow reports no linked repo', async () => {
    execute.mockResolvedValue({ found: false })
    getContactDetailsByPurls.mockResolvedValue([
      baseRow({
        resolvedRepositoryUrl: 'https://github.com/example/left-pad',
        contactsLastRefreshed: null,
      }),
    ])

    const { req, res } = mockReqRes({ purl: 'pkg:npm/left-pad' })

    await expect(ingestAkritesExternalContactDetail(req, res)).rejects.toThrow()
    expect(getContactDetailsByPurls).toHaveBeenCalledTimes(1)
  })

  it('throws NotFoundError when the re-read finds no row', async () => {
    execute.mockResolvedValue({ found: true })
    getContactDetailsByPurls
      .mockResolvedValueOnce([
        baseRow({
          resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
          contactsLastRefreshed: null,
        }),
      ])
      .mockResolvedValueOnce([])

    const { req, res } = mockReqRes({ purl: 'pkg:npm/lodash' })

    await expect(ingestAkritesExternalContactDetail(req, res)).rejects.toThrow()
  })

  it('rejects a request missing purl without executing a workflow', async () => {
    const { req, res } = mockReqRes({})

    await expect(ingestAkritesExternalContactDetail(req, res)).rejects.toThrow()
    expect(execute).not.toHaveBeenCalled()
  })
})
