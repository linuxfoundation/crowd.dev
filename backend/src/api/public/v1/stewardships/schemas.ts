import { z } from 'zod'

export const stewardshipIdParamsSchema = z.object({
  id: z.coerce.number().int().positive(),
})
