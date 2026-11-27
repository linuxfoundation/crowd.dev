import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError, ConflictError, ServiceUnavailableError } from '@crowd/common'

const sendSlackNotification = vi.hoisted(() => vi.fn())

vi.mock('@crowd/slack', () => ({
  SlackChannel: {},
  SlackPersona: {},
  sendSlackNotification,
}))

import ApiResponseHandler from './apiResponseHandler'

async function respondTo(error: Error) {
  const send = vi.fn()
  const res = { status: vi.fn(() => ({ send })) }
  const req = { url: '/event-mapping/confirm', method: 'POST', log: { error: vi.fn() } }
  await new ApiResponseHandler({ log: req.log } as never).error(req, res, error)
  return { status: res.status, send }
}

function errorWithCode(message: string, code: number) {
  return Object.assign(new Error(message), { code })
}

describe('ApiResponseHandler.error', () => {
  beforeEach(() => {
    sendSlackNotification.mockClear()
  })

  it.each([
    [new ConflictError('Sessionize event event-1 can no longer be confirmed!'), 409, false],
    [new BadRequestError('items: Array must contain at least 1 element(s)'), 400, false],
    [new ServiceUnavailableError('Upstream is down'), 500, true],
    [errorWithCode('Member not found', 404), 404, false],
    [new Error('Unexpected failure'), 500, true],
  ])('answers %s with status %i, alerting: %s', async (error, status, alerted) => {
    const response = await respondTo(error)

    expect(response.status).toHaveBeenCalledWith(status)
    expect(response.send).toHaveBeenCalledWith(error.message)
    expect(sendSlackNotification).toHaveBeenCalledTimes(alerted ? 1 : 0)
  })
})
