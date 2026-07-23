// Per-package fetch attempts. Shared between the workflow's Temporal retry policy and the
// activity's give-up threshold so the two never drift: a package is only given up on
// (marked scanned-error so the cursor can advance) once Temporal has exhausted these attempts.
export const INGEST_MAX_ATTEMPTS = 5
