import { z } from 'zod'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

// The reachability pipeline is npm-only for now — every other ecosystem (including
// a missing one) is rejected by the schema below before the Temporal workflow is
// triggered.
export const SUPPORTED_BLAST_RADIUS_ECOSYSTEMS = ['npm'] as const

// Always exactly one job per request — advisory-wide (package omitted) or narrowed
// to a single package. package accepts either a full purl or a bare package name,
// so it is NOT run through purlFieldSchema/normalizePurl like the other endpoints.
const ADVISORY_ID_PATTERN = /^(GHSA-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}|CVE-\d{4}-\d{4,})$/

// How recent a 'done' analysis for the same (advisoryId, package, ecosystem) has to
// be for submit to reuse it instead of starting a new Temporal workflow — see
// getRecentDoneAnalysis. Configurable via env so it can be tuned without a redeploy;
// defaults to 1 day. force=true on the request always bypasses this cache.
const blastRadiusCacheMaxAgeDaysEnv = Number(process.env.AKRITES_BLAST_RADIUS_CACHE_MAX_AGE_DAYS)
export const BLAST_RADIUS_CACHE_MAX_AGE_DAYS =
  Number.isSafeInteger(blastRadiusCacheMaxAgeDaysEnv) && blastRadiusCacheMaxAgeDaysEnv > 0
    ? blastRadiusCacheMaxAgeDaysEnv
    : 1

export const blastRadiusJobRequestSchema = z.object({
  advisoryId: z
    .string()
    .trim()
    .regex(ADVISORY_ID_PATTERN, 'advisoryId must be a GHSA or CVE identifier'),
  ecosystem: z.enum(SUPPORTED_BLAST_RADIUS_ECOSYSTEMS, {
    error: `Ecosystem is not supported for blast-radius analysis — only ${SUPPORTED_BLAST_RADIUS_ECOSYSTEMS.join(', ')} supported today`,
  }),
  package: z.string().trim().min(1).nullish(),
  force: z.boolean().default(false),
})

export type BlastRadiusJobRequest = z.infer<typeof blastRadiusJobRequestSchema>

export type BlastRadiusJobStatus = 'pending' | 'running' | 'done' | 'failed'

export type BlastRadiusJobEcosystem = (typeof SUPPORTED_BLAST_RADIUS_ECOSYSTEMS)[number]

export interface BlastRadiusJobEntry {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: BlastRadiusJobEcosystem
  status: BlastRadiusJobStatus
}

// Builds the 2a response body. status defaults to 'pending' (a freshly submitted
// job — see analyzeBlastRadius in packages_worker) but a cache hit passes the
// cached analysis's own status (always 'done' — see getRecentDoneAnalysis) so the
// caller doesn't need to poll a job that's already finished.
export function toBlastRadiusJobEntry(params: {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: BlastRadiusJobEcosystem
  status?: BlastRadiusJobStatus
}): BlastRadiusJobEntry {
  return {
    analysisId: params.analysisId,
    advisoryId: params.advisoryId,
    package: params.package,
    ecosystem: params.ecosystem,
    status: params.status ?? 'pending',
  }
}

// Shared by submitBlastRadiusJob and submitBlastRadiusJobBatch — looks up a
// recent 'done' analysis for the same (advisoryId, package, ecosystem) and, if
// found, builds the job entry for it. Returns null on a cache miss or when
// force=true (which bypasses the cache entirely).
export async function getCachedJobEntry(
  qx: QueryExecutor,
  params: {
    advisoryId: string
    package: string | null
    ecosystem: BlastRadiusJobEcosystem
    force: boolean
  },
): Promise<BlastRadiusJobEntry | null> {
  if (params.force) {
    return null
  }

  const cached = await blastRadiusDal.getRecentDoneAnalysis(
    qx,
    { advisoryOsvId: params.advisoryId, packageName: params.package, ecosystem: params.ecosystem },
    BLAST_RADIUS_CACHE_MAX_AGE_DAYS,
  )
  if (!cached) {
    return null
  }

  return toBlastRadiusJobEntry({
    analysisId: cached.id,
    advisoryId: params.advisoryId,
    package: params.package,
    ecosystem: params.ecosystem,
    status: 'done',
  })
}
