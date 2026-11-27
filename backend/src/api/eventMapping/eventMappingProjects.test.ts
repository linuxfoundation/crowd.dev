import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError } from '@crowd/common'

import Permissions from '../../security/permissions'

const QX = { executor: 'projects' }

const mocks = vi.hoisted(() => ({
  validateHas: vi.fn(),
  searchActiveSubprojectsForMapping: vi.fn(),
}))

vi.mock('../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = mocks.validateHas
  },
}))

vi.mock('@/database/repositories/sequelizeRepository', () => ({
  default: { getQueryExecutor: () => ({ executor: 'projects' }) },
}))

vi.mock('@crowd/data-access-layer/src/segments', () => ({
  searchActiveSubprojectsForMapping: mocks.searchActiveSubprojectsForMapping,
}))

import handler from './eventMappingProjects'

async function call(query: unknown) {
  const success = vi.fn()
  await handler({ query, responseHandler: { success } }, {})
  return success
}

describe('eventMappingProjects', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.searchActiveSubprojectsForMapping.mockResolvedValue({
      rows: [{ id: 'segment-1', name: 'Example Project', groupName: 'Example Foundation' }],
      total: 7,
    })
  })

  it('returns one page of projects with their project group', async () => {
    const success = await call({ search: 'example', limit: '20', offset: '40' })

    expect(mocks.validateHas).toHaveBeenCalledWith(Permissions.values.eventMappingRead)
    expect(mocks.searchActiveSubprojectsForMapping).toHaveBeenCalledWith(QX, {
      search: 'example',
      limit: 20,
      offset: 40,
    })
    const [, , payload] = success.mock.calls[0]
    expect(payload).toEqual({
      rows: [{ segmentId: 'segment-1', name: 'Example Project', groupName: 'Example Foundation' }],
      total: 7,
    })
  })

  it('pages the first 50 projects without a search by default', async () => {
    await call({})

    expect(mocks.searchActiveSubprojectsForMapping).toHaveBeenCalledWith(QX, {
      search: '',
      limit: 50,
      offset: 0,
    })
  })

  it.each([
    ['a limit of 0', { limit: '0' }],
    ['a limit above 100', { limit: '101' }],
    ['a negative offset', { offset: '-1' }],
    ['a search longer than 100 characters', { search: 'x'.repeat(101) }],
  ])('rejects %s', async (_name, query) => {
    await expect(call(query)).rejects.toBeInstanceOf(BadRequestError)
    expect(mocks.searchActiveSubprojectsForMapping).not.toHaveBeenCalled()
  })

  it('checks the permission before reading anything', async () => {
    mocks.validateHas.mockImplementation(() => {
      throw new Error('forbidden')
    })

    await expect(call({})).rejects.toThrow('forbidden')
    expect(mocks.searchActiveSubprojectsForMapping).not.toHaveBeenCalled()
  })
})
