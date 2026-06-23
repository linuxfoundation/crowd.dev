import { z } from 'zod'

export const actorInputSchema = z.object({
  username: z.string().trim().min(1).optional().nullable(),
  displayName: z.string().trim().min(1).optional().nullable(),
  avatarUrl: z.string().url().optional().nullable(),
})
