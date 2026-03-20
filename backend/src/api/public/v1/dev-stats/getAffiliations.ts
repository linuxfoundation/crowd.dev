import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  findMembersByGithubHandles,
  findVerifiedEmailsByMemberIds,
  optionsQx,
  resolveAffiliationsByMemberIds,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const log = getServiceChildLogger('dev-stats')

const MAX_HANDLES = 1000

const bodySchema = z.object({
  githubHandles: z
    .array(z.string().min(1))
    .min(1)
    .max(MAX_HANDLES, `Maximum ${MAX_HANDLES} handles per request`),
})

export async function getAffiliations(req: Request, res: Response): Promise<void> {
  const { githubHandles } = validateOrThrow(bodySchema, req.body)
  const qx = optionsQx(req)

  const t0 = performance.now()

  const lowercasedHandles = githubHandles.map((h) => h.toLowerCase())

  // Step 1: find verified members by github handles
  const memberRows = await findMembersByGithubHandles(qx, lowercasedHandles)

  const t1 = performance.now()
  log.info(
    { handles: githubHandles.length, found: memberRows.length, ms: Math.round(t1 - t0) },
    'Step 1: members lookup',
  )

  const foundHandles = new Set(memberRows.map((r) => r.githubHandle.toLowerCase()))
  const notFound = githubHandles.filter((h) => !foundHandles.has(h.toLowerCase()))

  if (memberRows.length === 0) {
    ok(res, { total_found: 0, contributors: [], notFound })
    return
  }

  const memberIds = memberRows.map((r) => r.memberId)

  // Step 2: fetch verified emails
  const emailRows = await findVerifiedEmailsByMemberIds(qx, memberIds)

  const t2 = performance.now()
  log.info(
    { members: memberIds.length, emails: emailRows.length, ms: Math.round(t2 - t1) },
    'Step 2: emails lookup',
  )

  const emailsByMember = new Map<string, string[]>()
  for (const row of emailRows) {
    const list = emailsByMember.get(row.memberId) ?? []
    list.push(row.email)
    emailsByMember.set(row.memberId, list)
  }

  // Step 3: resolve affiliations (conflict resolution, gap-filling, selection priority)
  const affiliationsByMember = await resolveAffiliationsByMemberIds(qx, memberIds)

  const t3 = performance.now()
  log.info(
    { members: memberIds.length, ms: Math.round(t3 - t2) },
    'Step 3: affiliations resolved',
  )

  // Step 4: build response
  const contributors = memberRows.map((member) => ({
    githubHandle: member.githubHandle,
    name: member.displayName,
    emails: emailsByMember.get(member.memberId) ?? [],
    affiliations: affiliationsByMember.get(member.memberId) ?? [],
  }))

  log.info(
    {
      handles: githubHandles.length,
      found: contributors.length,
      notFound: notFound.length,
      totalMs: Math.round(t3 - t0),
    },
    'dev-stats affiliations complete',
  )

  ok(res, { total_found: contributors.length, contributors, notFound })
}
