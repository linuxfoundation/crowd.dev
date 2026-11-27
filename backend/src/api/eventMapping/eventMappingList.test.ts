import { beforeEach, describe, expect, it, vi } from 'vitest'

import Permissions from '../../security/permissions'
import { SUBPROJECTS, heldEvent } from './eventMapping.fixture'

const mocks = vi.hoisted(() => ({
  validateHas: vi.fn(),
  listSessionizeEventsForReview: vi.fn(),
  listActiveSubprojectsForMapping: vi.fn(),
}))

vi.mock('../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = mocks.validateHas
  },
}))

vi.mock('./eventMappingQx', () => ({
  getEventMappingQx: async () => ({ executor: 'list' }),
}))

vi.mock('@crowd/data-access-layer/src/sessionize', () => ({
  listSessionizeEventsForReview: mocks.listSessionizeEventsForReview,
}))

vi.mock('@crowd/data-access-layer/src/segments', () => ({
  listActiveSubprojectsForMapping: mocks.listActiveSubprojectsForMapping,
}))

import handler from './eventMappingList'

async function call() {
  const success = vi.fn()
  await handler({ responseHandler: { success } }, {})
  return success
}

describe('eventMappingList', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.listSessionizeEventsForReview.mockResolvedValue([heldEvent('event-1')])
    mocks.listActiveSubprojectsForMapping.mockResolvedValue(SUBPROJECTS)
  })

  it('returns the review rows and their counts', async () => {
    const success = await call()

    expect(mocks.validateHas).toHaveBeenCalledWith(Permissions.values.eventMappingRead)
    const [, , payload] = success.mock.calls[0]
    expect(payload.rows.map((row) => [row.eventKey, row.state, row.match])).toEqual([
      ['event-1', 'suggested', 'same_name'],
    ])
    expect(payload.counts).toEqual({ suggested: 1, unresolved: 0, confirmed: 0, ignored: 0 })
  })

  it('checks the permission before reading anything', async () => {
    mocks.validateHas.mockImplementation(() => {
      throw new Error('forbidden')
    })

    await expect(call()).rejects.toThrow('forbidden')
    expect(mocks.listSessionizeEventsForReview).not.toHaveBeenCalled()
  })
})
