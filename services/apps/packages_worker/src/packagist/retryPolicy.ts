// Per-package fetch attempts. Shared between the workflow's Temporal retry policy and the
// activity's give-up threshold so the two never drift: a package is only given up on
// (marked scanned-error so the cursor can advance) once Temporal has exhausted these attempts.
export const INGEST_MAX_ATTEMPTS = 5

// Transitive prepare attempts. Same lockstep contract: the prepare activity only
// fail-marks its run row on the final attempt (or a non-retryable error); an earlier
// mark would make the row unadoptable and each retry would mint a duplicate.
export const TRANSITIVE_PREPARE_MAX_ATTEMPTS = 3
