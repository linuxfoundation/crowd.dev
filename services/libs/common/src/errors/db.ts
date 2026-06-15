type ConstraintCarrier = {
  constraint?: string
  original?: { constraint?: string }
  parent?: { constraint?: string }
}

/** Reads a Postgres constraint name from a pg or Sequelize-wrapped error. */
export function getDbConstraint(error: unknown): string | undefined {
  if (!error || typeof error !== 'object') {
    return undefined
  }

  const err = error as ConstraintCarrier
  return err.constraint ?? err.original?.constraint ?? err.parent?.constraint
}
