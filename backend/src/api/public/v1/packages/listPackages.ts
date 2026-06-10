import type { Request, Response } from 'express'
import { z } from 'zod'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  ecosystem: z.string().trim().optional(),
  lifecycle: z.string().trim().optional(),
  busFactor1Only: z.coerce.boolean().default(false),
  staleOnly: z.coerce.boolean().default(false),
  unstewardedOnly: z.coerce.boolean().default(false),
  sortBy: z.enum(['name', 'health', 'impact', 'openVulns']).default('name'),
  sortDir: z.enum(['asc', 'desc']).default('asc'),
})

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function listPackages(req: Request, res: Response): Promise<void> {
  const { page, pageSize, ecosystem, lifecycle, busFactor1Only, staleOnly, unstewardedOnly, sortBy, sortDir } =
    validateOrThrow(querySchema, req.query)

  ok(res, {
    page,
    pageSize,
    total: 0,
    filters: {
      ecosystem: ecosystem ?? null,
      lifecycle: lifecycle ?? null,
      busFactor1Only,
      staleOnly,
      unstewardedOnly,
    },
    sort: { by: sortBy, dir: sortDir },
    packages: [
      {
        purl: 'pkg:npm/%40lfx/example-package@1.0.0',
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
    ],
  })
}
