import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import {
  findMembersByGithubHandles,
  findVerifiedEmailsByMemberIds,
  optionsQx,
  resolveAffiliationsByMemberIds,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'

export async function getAffiliationByHandle(req: Request, res: Response): Promise<void> {
  const handle = req.params.githubHandle.toLowerCase()
  const qx = optionsQx(req)

  const members = await findMembersByGithubHandles(qx, [handle])
  if (members.length === 0) {
    throw new NotFoundError(`No LFX profile found for GitHub login '${req.params.githubHandle}'.`)
  }

  const member = members[0]
  const memberIds = [member.memberId]

  const [emailRows, affiliationsByMember] = await Promise.all([
    findVerifiedEmailsByMemberIds(qx, memberIds),
    resolveAffiliationsByMemberIds(qx, memberIds),
  ])

  ok(res, {
    githubHandle: member.githubHandle,
    name: member.displayName,
    emails: emailRows.map((r) => r.email),
    affiliations: affiliationsByMember.get(member.memberId) ?? [],
  })
}
