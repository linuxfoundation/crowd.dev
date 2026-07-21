import { z } from 'zod'

// The reachability pipeline is npm-only for now — every other ecosystem (including
// a missing one) is rejected by the schema below before the Temporal workflow is
// triggered.
export const SUPPORTED_BLAST_RADIUS_ECOSYSTEMS = ['npm'] as const

// Always exactly one job per request — advisory-wide (package omitted) or narrowed
// to a single package. package accepts either a full purl or a bare package name,
// so it is NOT run through purlFieldSchema/normalizePurl like the other endpoints.
const ADVISORY_ID_PATTERN = /^(GHSA-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}|CVE-\d{4}-\d{4,})$/

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

// Builds the 2a response body. The pipeline isn't implemented yet, so every freshly
// submitted job comes back pending — see analyzeBlastRadius in packages_worker.
export function toBlastRadiusJobEntry(params: {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: BlastRadiusJobEcosystem
}): BlastRadiusJobEntry {
  return {
    analysisId: params.analysisId,
    advisoryId: params.advisoryId,
    package: params.package,
    ecosystem: params.ecosystem,
    status: 'pending',
  }
}
