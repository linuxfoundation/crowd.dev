import { z } from 'zod'

export const eventReferenceSchema = z.object({
  platform: z.literal('sessionize'),
  eventKey: z.string().min(1),
})
