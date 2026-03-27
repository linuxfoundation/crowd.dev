import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  findMembersByGithubHandles,
  findVerifiedEmailsByMemberIds,
  optionsQx,
  resolveAffiliationsByMemberIds,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const MAX_HANDLES = 100
const DEFAULT_PAGE_SIZE = 20

const bodySchema = z.object({
  githubHandles: z
    .array(z.string().trim().min(1).toLowerCase())
    .min(1)
    .max(MAX_HANDLES, `Maximum ${MAX_HANDLES} handles per request`),
})

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_HANDLES).default(DEFAULT_PAGE_SIZE),
})

export async function getAffiliations(req: Request, res: Response): Promise<void> {
  const { githubHandles } = validateOrThrow(bodySchema, req.body)
  const { page, pageSize } = validateOrThrow(querySchema, req.query)
  const qx = optionsQx(req)

  const offset = (page - 1) * pageSize

  // Step 1: find all verified members across all handles
  const allMemberRows = await findMembersByGithubHandles(qx, githubHandles)

  const foundHandles = new Set(allMemberRows.map((r) => r.githubHandle.toLowerCase()))
  const notFound = githubHandles.filter((h) => !foundHandles.has(h))

  const pageMemberRows = allMemberRows.slice(offset, offset + pageSize)

  if (pageMemberRows.length === 0) {
    ok(res, {
      total: githubHandles.length,
      totalFound: allMemberRows.length,
      page,
      pageSize,
      contributorsInPage: 0,
      contributors: [],
      notFound,
    })
    return
  }

  const memberIds = pageMemberRows.map((r) => r.memberId)

  // Step 2: fetch verified emails for current page
  const emailRows = await findVerifiedEmailsByMemberIds(qx, memberIds)

  const emailsByMember = new Map<string, string[]>()
  for (const row of emailRows) {
    const list = emailsByMember.get(row.memberId) ?? []
    list.push(row.email)
    emailsByMember.set(row.memberId, list)
  }

  // Step 3: resolve affiliations for current page only
  const affiliationsByMember = await resolveAffiliationsByMemberIds(qx, memberIds)

  // Step 4: build response
  const contributors = pageMemberRows.map((member) => ({
    githubHandle: member.githubHandle,
    name: member.displayName,
    emails: emailsByMember.get(member.memberId) ?? [],
    affiliations: affiliationsByMember.get(member.memberId) ?? [],
  }))

  ok(res, {
    total: githubHandles.length,
    totalFound: allMemberRows.length,
    page,
    pageSize,
    contributorsInPage: contributors.length,
    contributors,
    notFound,
  })
}
