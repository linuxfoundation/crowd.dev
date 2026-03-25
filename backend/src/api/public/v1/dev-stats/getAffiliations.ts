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
const DEFAULT_PAGE_SIZE = 50

const bodySchema = z.object({
  githubHandles: z
    .array(z.string().min(1))
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

  const lowercasedHandles = githubHandles.map((h) => h.toLowerCase())

  const offset = (page - 1) * pageSize
  const pageHandles = lowercasedHandles.slice(offset, offset + pageSize)

  // Step 1: find verified members by github handles
  const memberRows = await findMembersByGithubHandles(qx, pageHandles)

  const foundHandles = new Set(memberRows.map((r) => r.githubHandle.toLowerCase()))
  const notFound = pageHandles.filter((h) => !foundHandles.has(h))

  if (memberRows.length === 0) {
    ok(res, {
      total: githubHandles.length,
      page,
      pageSize,
      total_found: 0,
      contributors: [],
      notFound,
    })
    return
  }

  const memberIds = memberRows.map((r) => r.memberId)

  // Step 2: fetch verified emails
  const emailRows = await findVerifiedEmailsByMemberIds(qx, memberIds)

  const emailsByMember = new Map<string, string[]>()
  for (const row of emailRows) {
    const list = emailsByMember.get(row.memberId) ?? []
    list.push(row.email)
    emailsByMember.set(row.memberId, list)
  }

  // Step 3: resolve affiliations (conflict resolution, gap-filling, selection priority)
  const affiliationsByMember = await resolveAffiliationsByMemberIds(qx, memberIds)

  // Step 4: build response
  const contributors = memberRows.map((member) => ({
    githubHandle: member.githubHandle,
    name: member.displayName,
    emails: emailsByMember.get(member.memberId) ?? [],
    affiliations: affiliationsByMember.get(member.memberId) ?? [],
  }))

  ok(res, {
    total: githubHandles.length,
    page,
    pageSize,
    total_found: contributors.length,
    contributors,
    notFound,
  })
}
