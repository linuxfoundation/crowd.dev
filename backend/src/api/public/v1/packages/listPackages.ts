import type { Request, Response } from 'express'
import { z } from 'zod'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { MOCK_DETAILS, MOCK_PACKAGES } from './mockData'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100
const STALE_MONTHS = 18

const booleanQueryParam = z.preprocess((v) => v === 'true', z.boolean()).default(false)

const lifecycleValues = ['active', 'stable', 'declining', 'abandoned'] as const

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  ecosystem: z.string().trim().optional(),
  lifecycle: z.enum(lifecycleValues).optional(),
  busFactor1Only: booleanQueryParam,
  staleOnly: booleanQueryParam,
  unstewardedOnly: booleanQueryParam,
  sortBy: z.enum(['name', 'health', 'impact', 'openVulns']).default('name'),
  sortDir: z.enum(['asc', 'desc']).default('asc'),
})

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function listPackages(req: Request, res: Response): Promise<void> {
  const {
    page,
    pageSize,
    ecosystem,
    lifecycle,
    busFactor1Only,
    staleOnly,
    unstewardedOnly,
    sortBy,
    sortDir,
  } = validateOrThrow(querySchema, req.query)

  const staleThreshold = new Date()
  staleThreshold.setMonth(staleThreshold.getMonth() - STALE_MONTHS)

  let filtered = MOCK_PACKAGES.filter((p) => {
    if (ecosystem && p.ecosystem !== ecosystem) return false
    if (lifecycle && p.lifecycle !== lifecycle) return false
    if (busFactor1Only && p.maintainerBusFactor !== 1) return false
    if (unstewardedOnly && p.stewardship !== null && p.stewardship !== 'unassigned') return false
    if (staleOnly) {
      const lastRelease = MOCK_DETAILS[p.purl]?.general.riskSignals.lastRelease
      if (!lastRelease || new Date(lastRelease) >= staleThreshold) return false
    }
    return true
  })

  filtered = filtered.sort((a, b) => {
    let cmp = 0
    if (sortBy === 'name') {
      cmp = a.name.localeCompare(b.name)
    } else if (sortBy === 'health') {
      cmp = (a.health ?? 0) - (b.health ?? 0)
    } else if (sortBy === 'impact') {
      cmp = (a.impact ?? 0) - (b.impact ?? 0)
    } else if (sortBy === 'openVulns') {
      const sumA = a.openVulns.low + a.openVulns.medium + a.openVulns.high + a.openVulns.critical
      const sumB = b.openVulns.low + b.openVulns.medium + b.openVulns.high + b.openVulns.critical
      cmp = sumA - sumB
    }
    return sortDir === 'desc' ? -cmp : cmp
  })

  const total = filtered.length
  const start = (page - 1) * pageSize
  const packages = filtered.slice(start, start + pageSize)

  ok(res, {
    page,
    pageSize,
    total,
    filters: {
      ecosystem: ecosystem ?? null,
      lifecycle: lifecycle ?? null,
      busFactor1Only,
      staleOnly,
      unstewardedOnly,
    },
    sort: { by: sortBy, dir: sortDir },
    packages,
  })
}
