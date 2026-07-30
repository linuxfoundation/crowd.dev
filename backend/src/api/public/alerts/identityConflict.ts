import type { Request } from 'express'

import { ConflictError } from '@crowd/common'
import type { IMemberIdentity } from '@crowd/types'

import { notifyOnce } from '@/api/public/alerts/notifyOnce'
import { rethrowDbConflict } from '@/utils/err'

type IdentityConflictSubject = Pick<IMemberIdentity, 'memberId' | 'platform' | 'value' | 'type'>

function notifyIdentityConflict(
  req: Request,
  identity: IdentityConflictSubject,
  message: string,
): void {
  const dedupeKey = [
    'member-identity-conflict',
    identity.platform,
    identity.type,
    identity.value,
    identity.memberId,
  ]
    .filter(Boolean)
    .join(':')

  const context = {
    platform: identity.platform,
    value: identity.value,
    type: identity.type,
    ...(identity.memberId ? { memberId: identity.memberId } : {}),
  }

  void notifyOnce(req, dedupeKey, 'Public API Identity Conflict 409', [
    {
      title: 'Request',
      text: `*Method:* \`${req.method}\`\n*URL:* \`${req.originalUrl}\``,
    },
    { title: 'Conflict', text: `*Message:* ${message}` },
    { title: 'Context', text: `\`\`\`${JSON.stringify(context, null, 2)}\`\`\`` },
  ])
}

/** Maps identity unique violations to ConflictError and alerts once. */
export function rethrowIdentityConflict(
  req: Request,
  error: unknown,
  identity: IdentityConflictSubject,
): never {
  try {
    rethrowDbConflict(error, {
      platform: identity.platform,
      value: identity.value,
      type: identity.type,
    })
  } catch (e) {
    if (e instanceof ConflictError) {
      notifyIdentityConflict(req, identity, e.message)
    }
    throw e
  }
}
