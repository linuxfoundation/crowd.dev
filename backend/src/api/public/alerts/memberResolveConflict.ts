import type { Request } from 'express'

import { ConflictError } from '@crowd/common'

import { notifyOnce } from '@/api/public/alerts/notifyOnce'

function notifyMemberResolveConflict(req: Request, memberIds: string[], message: string): void {
  const dedupeKey = `member-resolve:${[...memberIds].sort().join(':')}`

  void notifyOnce(req, dedupeKey, 'Member resolve conflict', [
    {
      title: 'Members',
      text: memberIds.map((id) => `• \`${id}\``).join('\n'),
    },
    { title: 'Conflict', text: `*Message:* ${message}` },
    {
      title: 'Request',
      text: `*Method:* \`${req.method}\`\n*URL:* \`${req.url}\``,
    },
  ])
}

/** Throws ConflictError for ambiguous resolve and alerts once. */
export function throwMemberResolveConflict(req: Request, memberIds: string[]): never {
  const message = 'Multiple member profiles matched'
  notifyMemberResolveConflict(req, memberIds, message)
  throw new ConflictError(message, { memberIds })
}
