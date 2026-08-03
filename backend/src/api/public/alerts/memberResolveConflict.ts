import type { Request } from 'express'

import { ConflictError } from '@crowd/common'

import { notifyOnce } from '@/api/public/alerts/notifyOnce'

function notifyMemberResolveConflict(req: Request, memberIds: string[], message: string): void {
  const dedupeKey = `member-resolve:${[...memberIds].sort().join(':')}`

  void notifyOnce(req, dedupeKey, 'Public API Member Resolve Conflict 409', [
    {
      title: 'Request',
      text: `*Method:* \`${req.method}\`\n*URL:* \`${req.originalUrl}\``,
    },
    { title: 'Conflict', text: `*Message:* ${message}` },
    {
      title: 'Context',
      text: `\`\`\`${JSON.stringify({ memberIds }, null, 2)}\`\`\``,
    },
  ])
}

/** Throws ConflictError for ambiguous resolve and alerts once. */
export function throwMemberResolveConflict(req: Request, memberIds: string[]): never {
  const message = 'Multiple member profiles matched'
  notifyMemberResolveConflict(req, memberIds, message)
  throw new ConflictError(message, { memberIds })
}
