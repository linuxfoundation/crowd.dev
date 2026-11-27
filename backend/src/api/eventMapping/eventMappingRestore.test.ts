import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError } from '@crowd/common'

import Permissions from '../../security/permissions'

const QX = { executor: 'restore' }

const mocks = vi.hoisted(() => ({
  validateHas: vi.fn(),
  restoreSessionizeEvent: vi.fn(),
}))

vi.mock('../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = mocks.validateHas
  },
}))

vi.mock('./eventMappingQx', () => ({
  getEventMappingQx: async () => ({ executor: 'restore' }),
}))

vi.mock('@crowd/data-access-layer/src/sessionize', () => ({
  restoreSessionizeEvent: mocks.restoreSessionizeEvent,
}))

import handler from './eventMappingRestore'

async function call(body: unknown) {
  const success = vi.fn()
  await handler({ body, currentUser: { id: 'user-1' }, responseHandler: { success } }, {})
  return success
}

describe('eventMappingRestore', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.restoreSessionizeEvent.mockResolvedValue(undefined)
  })

  it('puts the event back on hold', async () => {
    const success = await call({ platform: 'sessionize', eventKey: 'event-1' })

    expect(mocks.validateHas).toHaveBeenCalledWith(Permissions.values.eventMappingEdit)
    expect(mocks.restoreSessionizeEvent).toHaveBeenCalledWith(QX, 'event-1')
    expect(success).toHaveBeenCalled()
  })

  it.each([
    ['another platform', { platform: 'luma', eventKey: 'event-1' }],
    ['an empty event key', { platform: 'sessionize', eventKey: '' }],
    ['no body', undefined],
  ])('rejects %s', async (_name, body) => {
    await expect(call(body)).rejects.toBeInstanceOf(BadRequestError)
    expect(mocks.restoreSessionizeEvent).not.toHaveBeenCalled()
  })

  it('checks the permission before anything else', async () => {
    mocks.validateHas.mockImplementation(() => {
      throw new Error('forbidden')
    })

    await expect(call({ platform: 'sessionize', eventKey: 'event-1' })).rejects.toThrow('forbidden')
    expect(mocks.restoreSessionizeEvent).not.toHaveBeenCalled()
  })
})
