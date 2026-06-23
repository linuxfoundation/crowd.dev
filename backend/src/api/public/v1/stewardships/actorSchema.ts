import { z } from 'zod'

export const actorInputSchema = z.object({
  userId: z.string().trim().min(1, { message: 'actor.userId is required and must not be empty' }),
  username: z.string().trim().min(1).optional().nullable(),
  displayName: z.string().trim().min(1).optional().nullable(),
  avatarUrl: z.string().url().optional().nullable(),
})
