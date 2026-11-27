import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError } from '@crowd/common'

import Permissions from '../../security/permissions'

const QX = { executor: 'ignore' }

const mocks = vi.hoisted(() => ({
  validateHas: vi.fn(),
  ignoreSessionizeEvent: vi.fn(),
}))

vi.mock('../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = mocks.validateHas
  },
}))

vi.mock('./eventMappingQx', () => ({
  getEventMappingQx: async () => ({ executor: 'ignore' }),
}))

vi.mock('@crowd/data-access-layer/src/sessionize', () => ({
  ignoreSessionizeEvent: mocks.ignoreSessionizeEvent,
}))

import handler from './eventMappingIgnore'

async function call(body: unknown) {
  const success = vi.fn()
  await handler({ body, currentUser: { id: 'user-1' }, responseHandler: { success } }, {})
  return success
}

describe('eventMappingIgnore', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.ignoreSessionizeEvent.mockResolvedValue(undefined)
  })

  it('ignores the event as the current user', async () => {
    const success = await call({ platform: 'sessionize', eventKey: 'event-1' })

    expect(mocks.validateHas).toHaveBeenCalledWith(Permissions.values.eventMappingEdit)
    expect(mocks.ignoreSessionizeEvent).toHaveBeenCalledWith(QX, {
      eventKey: 'event-1',
      reviewedBy: 'user-1',
    })
    expect(success).toHaveBeenCalled()
  })

  it.each([
    ['another platform', { platform: 'luma', eventKey: 'event-1' }],
    ['an empty event key', { platform: 'sessionize', eventKey: '' }],
    ['no body', undefined],
  ])('rejects %s', async (_name, body) => {
    await expect(call(body)).rejects.toBeInstanceOf(BadRequestError)
    expect(mocks.ignoreSessionizeEvent).not.toHaveBeenCalled()
  })

  it('checks the permission before anything else', async () => {
    mocks.validateHas.mockImplementation(() => {
      throw new Error('forbidden')
    })

    await expect(call({ platform: 'sessionize', eventKey: 'event-1' })).rejects.toThrow('forbidden')
    expect(mocks.ignoreSessionizeEvent).not.toHaveBeenCalled()
  })
})
