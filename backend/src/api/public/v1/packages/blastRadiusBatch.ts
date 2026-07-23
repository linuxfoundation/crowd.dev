import { z } from 'zod'

import { blastRadiusJobRequestSchema } from './blastRadius'
import type { BlastRadiusAnalysis } from './blastRadiusAnalysis'
import { DEFAULT_BATCH_PAGE_SIZE } from './purl'

// Read-only batches (packages/advisories/contacts) cap at 100 — cheap indexed
// lookups. Batch submit multiplies Temporal workflow starts (and their LLM
// reachability cost) per request, so it gets a much lower cap, independent of
// that constant. 20 is the hard limit agreed with Joana after the cost test
// (2026-07-23) — 10 is the recommended/default batch size for callers, not a
// separate enforced value, since `jobs` is an explicit client-provided array.
export const MAX_BLAST_RADIUS_JOBS_PER_BATCH = 20

// Polling is read-only, same cost profile as the other batches, so it reuses
// their 100 cap.
export const MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH = 100

export const blastRadiusJobBatchRequestSchema = z.object({
  jobs: z
    .array(blastRadiusJobRequestSchema)
    .min(1)
    .max(
      MAX_BLAST_RADIUS_JOBS_PER_BATCH,
      `Maximum ${MAX_BLAST_RADIUS_JOBS_PER_BATCH} jobs per request`,
    ),
})

export type BlastRadiusJobBatchRequest = z.infer<typeof blastRadiusJobBatchRequestSchema>

// Unlike the read batches (purls that may or may not resolve to a package), every
// job in a submit batch is genuinely submitted — there is no "not found" case, so
// the response is a plain array in request order, not a found/not-found wrapper.
const analysisIdSchema = z.uuid()

export const blastRadiusJobPollBatchRequestSchema = z.object({
  analysisIds: z
    .array(analysisIdSchema)
    .min(1)
    .max(
      MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH,
      `Maximum ${MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH} analysisIds per request`,
    ),
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce
    .number()
    .int()
    .min(1)
    .max(MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH)
    .default(DEFAULT_BATCH_PAGE_SIZE),
})

export type BlastRadiusJobPollBatchRequest = z.infer<typeof blastRadiusJobPollBatchRequestSchema>

export interface BlastRadiusAnalysisBulkEntry {
  requestedAnalysisId: string
  found: boolean
  analysis: BlastRadiusAnalysis | null
}

export interface PaginatedAnalysisIds {
  page: number
  pageSize: number
  total: number
  pagedAnalysisIds: string[]
}

// Mirrors paginatePurls (purl.ts): slice the requested page out of the full
// analysisIds array. No normalization step — analysisIds are UUIDs, unlike purls.
export function paginateAnalysisIds(body: {
  analysisIds: string[]
  page: number
  pageSize: number
}): PaginatedAnalysisIds {
  const { analysisIds, page, pageSize } = body
  const start = (page - 1) * pageSize
  return {
    page,
    pageSize,
    total: analysisIds.length,
    pagedAnalysisIds: analysisIds.slice(start, start + pageSize),
  }
}
