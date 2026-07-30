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

  void notifyOnce(req, dedupeKey, 'Identity conflict', [
    {
      title: 'Identity',
      text: `*Platform:* \`${identity.platform}\`\n*Type:* \`${identity.type}\`\n*Value:* \`${identity.value}\``,
    },
    ...(identity.memberId
      ? [{ title: 'Member', text: `*Member ID:* \`${identity.memberId}\`` }]
      : []),
    { title: 'Conflict', text: `*Message:* ${message}` },
    {
      title: 'Request',
      text: `*Method:* \`${req.method}\`\n*URL:* \`${req.url}\``,
    },
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
