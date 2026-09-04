import { deriveProjectSlug } from '../onboarder/onboarder'

// A soft-deleted insightsProjects row still owns its slug (the unique index can't be made
// partial on deletedAt: three FKs reference it), so segment creation would 500 on it.
export function buildInsightsProjectSkipReason(projectSlug: string, deletedAt: string): string {
  const slug = deriveProjectSlug(projectSlug)
  return `Insights project '${slug}' was deleted on ${deletedAt}; onboarding skipped for manual review`
}
