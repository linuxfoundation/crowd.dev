import type { Request } from 'express'

import { generateUUIDv4 } from '@crowd/common'
import { RedisCache } from '@crowd/redis'
import {
  SlackChannel,
  type SlackMessageSection,
  SlackPersona,
  sendSlackNotification,
} from '@crowd/slack'

/** Sends a Slack alert once per dedupe key. */
export async function notifyOnce(
  req: Request,
  key: string,
  title: string,
  sections: SlackMessageSection[],
): Promise<void> {
  const cache = new RedisCache('public-api-alerts', req.redis, req.log)
  const token = generateUUIDv4()

  try {
    const result = await cache.setIfNotExistsOrGet(key, token, 60 * 60)

    if (result !== token) {
      req.log.info({ key }, 'Skipping duplicate public API alert')
      return
    }
  } catch (err) {
    req.log.warn({ err, key }, 'Failed to deduplicate, sending alert')
  }

  sendSlackNotification(
    SlackChannel.CDP_LFX_SELF_SERVE_ALERTS,
    SlackPersona.WARNING_PROPAGATOR,
    title,
    sections,
  )
}
