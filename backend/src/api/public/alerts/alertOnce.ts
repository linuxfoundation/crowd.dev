import { createHash } from 'crypto'
import type { Request } from 'express'

import { generateUUIDv4 } from '@crowd/common'
import { RedisCache } from '@crowd/redis'
import {
  SlackChannel,
  type SlackMessageSection,
  SlackPersona,
  sendSlackNotification,
} from '@crowd/slack'

const PATH_UUID =
  /[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi

export async function alertOnce(
  req: Request,
  {
    status,
    code,
    message,
    name,
    context,
    stack,
  }: {
    status: number
    code: string
    message: string
    name?: string
    context?: Record<string, unknown>
    stack?: string
  },
): Promise<void> {
  if (status !== 409 && status < 500) return

  const path = (req.originalUrl || req.url || '').split('?')[0]

  // akrites alerts are handled separately, so skip them here.
  if (
    path.startsWith('/v1/akrites') ||
    path.startsWith('/v1/akrites-external')
  ) {
    return
  }

  const route = resolveRoute(req)

  const dedupeKey = createHash('sha256')
    .update(
      [status, req.method, route, code, message, serializeContext(context)]
        .filter((part) => part !== '')
        .join(':'),
    )
    .digest('hex')

  const cache = new RedisCache('public-api-alerts', req.redis, req.log)
  const lease = generateUUIDv4()

  try {
    const held = await cache.setIfNotExistsOrGet(dedupeKey, lease, 60 * 60)
    if (held !== lease) {
      req.log.info({ dedupeKey }, 'Skipping duplicate public API alert')
      return
    }
  } catch (err) {
    req.log.warn({ err, dedupeKey }, 'Alert dedupe failed; sending anyway')
  }

  const sections: SlackMessageSection[] = [
    {
      title: 'Request',
      text: `*Method:* \`${req.method}\`\n*URL:* \`${req.originalUrl || req.url}\``,
    },
    {
      title: 'Error',
      text: `*Code:* \`${code}\`\n*Name:* \`${name || code}\`\n*Message:* ${message}`,
    },
  ]

  if (context && Object.keys(context).length > 0) {
    sections.push({
      title: 'Context',
      text: `\`\`\`${JSON.stringify(context, null, 2)}\`\`\``,
    })
  }

  if (stack) {
    sections.push({
      title: 'Stack Trace',
      text: `\`\`\`${stack.substring(0, 2700)}\`\`\``,
    })
  }

  sendSlackNotification(
    SlackChannel.CDP_PUBLIC_API_ALERTS,
    status >= 500 ? SlackPersona.ERROR_REPORTER : SlackPersona.WARNING_PROPAGATOR,
    status >= 500
      ? `500 Error: ${name || message}`
      : `${status} Conflict: ${message}`,
    sections,
  )
}

function resolveRoute(req: Request): string {
  if (req.route?.path != null) {
    return `${req.baseUrl}${req.route.path}`
  }
  return (req.path || '').replace(PATH_UUID, ':id')
}

function serializeContext(context?: Record<string, unknown>): string {
  if (!context) return ''

  return Object.keys(context)
    .sort()
    .map((key) => {
      const value = context[key]
      if (Array.isArray(value)) {
        return `${key}=${[...value].map(String).sort().join(',')}`
      }
      if (value !== null && typeof value === 'object') {
        return `${key}=${JSON.stringify(value)}`
      }
      return `${key}=${String(value)}`
    })
    .join('|')
}
