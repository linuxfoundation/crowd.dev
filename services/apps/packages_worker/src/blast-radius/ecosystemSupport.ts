import { ApplicationFailure } from '@temporalio/workflow'

// Single source of truth for supported ecosystems — kept in this leaf, I/O-free file
// (no activities/DAL imports) so the workflow bundle stays deterministic-safe.
export const SUPPORTED_ECOSYSTEMS = ['npm', 'go', 'maven', 'cargo'] as const
export type Ecosystem = (typeof SUPPORTED_ECOSYSTEMS)[number]

// Pure so it's testable outside the workflow sandbox (Workflow.log/context calls
// throw when invoked outside a running workflow — this helper makes none).
export function buildEcosystemNotSupportedFailure(ecosystem: string | null): ApplicationFailure {
  return ApplicationFailure.nonRetryable(
    `Blast-radius analysis not supported for ecosystem "${ecosystem ?? 'unknown'}"`,
    'ECOSYSTEM_NOT_SUPPORTED',
  )
}
