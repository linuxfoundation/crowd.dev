import { z } from 'zod'

// Advisory-wide when package is omitted; narrowed to one package otherwise.
// Mirrors BlastRadiusJobRequest in the akrites-external draft contract.
export const blastRadiusJobRequestSchema = z.object({
  advisoryId: z.string().trim().min(1),
  ecosystem: z.string().trim().min(1).nullable().optional(),
  package: z.string().trim().min(1).nullable().optional(),
  force: z.boolean().optional().default(false),
})

export const analysisIdParamSchema = z.object({
  analysisId: z.string().trim().min(1),
})
