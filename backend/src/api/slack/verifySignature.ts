import { createHmac, timingSafeEqual } from 'crypto'

import { getSlackBotConfig } from '@crowd/slack'

const MAX_REQUEST_AGE_SECONDS = 60 * 5

export function verifySlackSignature(req): boolean {
  const { signingSecret } = getSlackBotConfig()
  if (!signingSecret) {
    return false
  }

  const timestamp = req.headers['x-slack-request-timestamp']
  const signature = req.headers['x-slack-signature']
  if (!timestamp || !signature || !req.rawBody) {
    return false
  }

  const age = Math.abs(Math.floor(Date.now() / 1000) - Number(timestamp))
  if (!Number.isFinite(age) || age > MAX_REQUEST_AGE_SECONDS) {
    return false
  }

  const baseString = `v0:${timestamp}:${req.rawBody}`
  const expectedSignature = `v0=${createHmac('sha256', signingSecret).update(baseString).digest('hex')}`

  const expected = Buffer.from(expectedSignature)
  const actual = Buffer.from(signature)
  return expected.length === actual.length && timingSafeEqual(expected, actual)
}
