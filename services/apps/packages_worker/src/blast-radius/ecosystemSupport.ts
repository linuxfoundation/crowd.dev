import { ApplicationFailure } from '@temporalio/workflow'

// Pure so it's testable outside the workflow sandbox (Workflow.log/context calls
// throw when invoked outside a running workflow — this helper makes none).
// The reachability pipeline isn't built yet — npm is the only ecosystem that will
// eventually get true reachability analysis, others a basic scoring result — so
// every ecosystem fails today, npm included.
export function buildEcosystemNotSupportedFailure(ecosystem: string | null): ApplicationFailure {
  return ApplicationFailure.nonRetryable(
    `Blast-radius analysis not supported for ecosystem "${ecosystem ?? 'unknown'}"`,
    'ECOSYSTEM_NOT_SUPPORTED',
  )
}
