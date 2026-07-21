import { z } from 'zod'

// The reachability pipeline is npm-only for now — every other ecosystem (including
// missing/null) is rejected at the API layer before the Temporal workflow is triggered.
export const SUPPORTED_BLAST_RADIUS_ECOSYSTEMS = ['npm'] as const

export type SupportedBlastRadiusEcosystem = (typeof SUPPORTED_BLAST_RADIUS_ECOSYSTEMS)[number]

export function isSupportedBlastRadiusEcosystem(
  ecosystem: string | null | undefined,
): ecosystem is SupportedBlastRadiusEcosystem {
  return (SUPPORTED_BLAST_RADIUS_ECOSYSTEMS as readonly string[]).includes(ecosystem ?? '')
}

// Always exactly one job per request — advisory-wide (package omitted) or narrowed
// to a single package. package accepts either a full purl or a bare package name,
// so it is NOT run through purlFieldSchema/normalizePurl like the other endpoints.
export const blastRadiusJobRequestSchema = z.object({
  advisoryId: z.string().trim().min(1, 'advisoryId is required'),
  ecosystem: z.string().trim().min(1).nullish(),
  package: z.string().trim().min(1).nullish(),
  force: z.boolean().default(false),
})

export type BlastRadiusJobRequest = z.infer<typeof blastRadiusJobRequestSchema>

export type BlastRadiusJobStatus = 'pending' | 'running' | 'done' | 'failed'

export interface BlastRadiusJobEntry {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: string | null
  status: BlastRadiusJobStatus
}

// Builds the 2a response body. The pipeline isn't implemented yet, so every freshly
// submitted job comes back pending — see analyzeBlastRadius in packages_worker.
export function toBlastRadiusJobEntry(params: {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: string | null
}): BlastRadiusJobEntry {
  return {
    analysisId: params.analysisId,
    advisoryId: params.advisoryId,
    package: params.package,
    ecosystem: params.ecosystem,
    status: 'pending',
  }
}
