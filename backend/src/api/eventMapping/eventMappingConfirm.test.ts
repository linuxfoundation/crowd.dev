import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError, ConflictError } from '@crowd/common'

import Permissions from '../../security/permissions'
import {
  EXAMPLE_SEGMENT_ID,
  OTHER_SEGMENT_ID,
  SUBPROJECTS,
  heldEvent,
} from './eventMapping.fixture'

const mocks = vi.hoisted(() => ({
  validateHas: vi.fn(),
  tx: vi.fn(),
  listSessionizeEventsForReview: vi.fn(),
  confirmSessionizeEventMappings: vi.fn(),
  listActiveSubprojectsForMapping: vi.fn(),
}))

vi.mock('../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = mocks.validateHas
  },
}))

vi.mock('@/database/repositories/sequelizeRepository', () => ({
  default: { getQueryExecutor: () => ({ tx: mocks.tx }) },
}))

vi.mock('@crowd/data-access-layer/src/sessionize', () => ({
  listSessionizeEventsForReview: mocks.listSessionizeEventsForReview,
  confirmSessionizeEventMappings: mocks.confirmSessionizeEventMappings,
}))

vi.mock('@crowd/data-access-layer/src/segments', () => ({
  listActiveSubprojectsForMapping: mocks.listActiveSubprojectsForMapping,
}))

import handler from './eventMappingConfirm'

const TX = { transaction: 'confirm' }

function item(eventKey: string, segmentId = EXAMPLE_SEGMENT_ID) {
  return { platform: 'sessionize', eventKey, segmentId }
}

async function call(body: unknown) {
  const success = vi.fn()
  const req = { body, currentUser: { id: 'user-1' }, responseHandler: { success } }
  await handler(req, {})
  return success
}

describe('eventMappingConfirm', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.tx.mockImplementation((fn) => fn(TX))
    mocks.listSessionizeEventsForReview.mockResolvedValue([
      heldEvent('event-1'),
      heldEvent('event-2'),
    ])
    mocks.listActiveSubprojectsForMapping.mockResolvedValue(SUBPROJECTS)
    mocks.confirmSessionizeEventMappings.mockResolvedValue(undefined)
  })

  it('confirms every item in one transaction as the current user', async () => {
    const success = await call({
      items: [
        item('event-1', EXAMPLE_SEGMENT_ID.toUpperCase()),
        item('event-2', OTHER_SEGMENT_ID),
        item('not-listed'),
      ],
    })

    expect(mocks.validateHas).toHaveBeenCalledWith(Permissions.values.eventMappingEdit)
    expect(mocks.listSessionizeEventsForReview).toHaveBeenCalledWith(TX)
    expect(mocks.listActiveSubprojectsForMapping).toHaveBeenCalledWith(TX)
    expect(mocks.confirmSessionizeEventMappings).toHaveBeenCalledWith(TX, [
      {
        eventKey: 'event-1',
        segmentId: EXAMPLE_SEGMENT_ID,
        cventEventId: 'cvent-event-1',
        catalogueSource: 'cvent',
        reviewedBy: 'user-1',
      },
      {
        eventKey: 'event-2',
        segmentId: OTHER_SEGMENT_ID,
        cventEventId: null,
        catalogueSource: null,
        reviewedBy: 'user-1',
      },
      {
        eventKey: 'not-listed',
        segmentId: EXAMPLE_SEGMENT_ID,
        cventEventId: null,
        catalogueSource: null,
        reviewedBy: 'user-1',
      },
    ])
    expect(success).toHaveBeenCalled()
  })

  it('lets a conflict from the database fail the whole request', async () => {
    mocks.confirmSessionizeEventMappings.mockRejectedValue(new ConflictError('taken'))

    await expect(call({ items: [item('event-1')] })).rejects.toBeInstanceOf(ConflictError)
  })

  it('refuses a segment that is not an active subproject', async () => {
    await expect(
      call({ items: [item('event-1', '00000000-0000-4000-8000-000000000000')] }),
    ).rejects.toBeInstanceOf(BadRequestError)
    expect(mocks.confirmSessionizeEventMappings).not.toHaveBeenCalled()
  })

  it.each([
    ['an empty list', { items: [] }],
    ['201 items', { items: Array.from({ length: 201 }, (_, i) => item(`event-${i}`)) }],
    ['a segment that is not a uuid', { items: [item('event-1', 'segment-1')] }],
    ['a duplicate event key', { items: [item('event-1'), item('event-1', OTHER_SEGMENT_ID)] }],
    ['another platform', { items: [{ ...item('event-1'), platform: 'luma' }] }],
    ['an empty event key', { items: [item('')] }],
    ['no body', undefined],
  ])('rejects %s before touching the database', async (_name, body) => {
    await expect(call(body)).rejects.toBeInstanceOf(BadRequestError)
    expect(mocks.tx).not.toHaveBeenCalled()
  })

  it('checks the permission before anything else', async () => {
    mocks.validateHas.mockImplementation(() => {
      throw new Error('forbidden')
    })

    await expect(call({ items: [item('event-1')] })).rejects.toThrow('forbidden')
    expect(mocks.tx).not.toHaveBeenCalled()
  })
})
