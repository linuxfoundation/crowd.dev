import { ConflictError, getDbConstraint } from '@crowd/common'

type ConflictFactory = (context?: Record<string, unknown>) => Error

const DB_CONFLICT_MAP: Record<string, ConflictFactory> = {
  uix_memberIdentities_memberId_platform_type_lower_value: (context) =>
    new ConflictError('Identity already exists on this member', context),
  uix_memberIdentities_platform_type_lower_value_verified: (context) =>
    new ConflictError('Identity already exists on another member', context),
}

export function isMemberIdentityDbConflict(error: unknown): boolean {
  return (getDbConstraint(error) ?? '') in DB_CONFLICT_MAP
}

export function rethrowDbConflict(error: unknown, context?: Record<string, unknown>): never {
  const factory = DB_CONFLICT_MAP[getDbConstraint(error) ?? '']

  if (factory) {
    throw factory(context)
  }

  throw error
}
