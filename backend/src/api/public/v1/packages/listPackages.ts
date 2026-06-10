import type { Request, Response } from 'express'
import { z } from 'zod'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

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

const MOCK_PACKAGES = [
  {
    purl: 'pkg:npm/%40lfx%2Fexample-package@1.0.0',
    name: 'example-package',
    ecosystem: 'npm',
    health: 18,
    impact: 71,
    lifecycle: 'declining',
    maintainerBusFactor: 1,
    openVulns: { low: 0, medium: 0, high: 1, critical: 0 },
    stewardship: 'unassigned',
    steward: null,
  },
]

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function listPackages(req: Request, res: Response): Promise<void> {
  const { page, pageSize, ecosystem, lifecycle, busFactor1Only, staleOnly, unstewardedOnly, sortBy, sortDir } =
    validateOrThrow(querySchema, req.query)

  ok(res, {
    page,
    pageSize,
    total: MOCK_PACKAGES.length,
    filters: {
      ecosystem: ecosystem ?? null,
      lifecycle: lifecycle ?? null,
      busFactor1Only,
      staleOnly,
      unstewardedOnly,
    },
    sort: { by: sortBy, dir: sortDir },
    packages: MOCK_PACKAGES,
  })
}
