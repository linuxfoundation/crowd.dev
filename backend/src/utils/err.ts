import { ConflictError, getDbConstraint } from '@crowd/common'

type ConflictFactory = (context?: Record<string, unknown>) => Error

const DB_CONFLICT_MAP: Record<string, ConflictFactory> = {
  uix_memberIdentities_platform_value_type_verified: (context) =>
    new ConflictError('Identity already exists on another member', context),
}

export function rethrowDbConflict(error: unknown, context?: Record<string, unknown>): never {
  const factory = DB_CONFLICT_MAP[getDbConstraint(error) ?? '']

  if (factory) {
    throw factory(context)
  }

  throw error
}
