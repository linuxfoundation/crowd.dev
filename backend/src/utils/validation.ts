import { z } from 'zod'

import { BadRequestError } from '@crowd/common'

export function validateOrThrow<T extends z.Schema>(schema: T, data: unknown): z.infer<T> {
  const result = schema.safeParse(data)

  if (!result.success) {
    const messages = result.error.issues.map((issue) => {
      const path = issue.path.length ? `${issue.path.join('.')}: ` : ''
      return `${path}${issue.message}`
    })
    throw new BadRequestError(messages.join('; '))
  }

  return result.data
}
