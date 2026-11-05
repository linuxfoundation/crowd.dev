import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { joinsExistingProject } from '@crowd/project-onboarding'

// A soft-deleted insightsProjects row still owns its slug (the unique index can't be made
// partial on deletedAt: three FKs reference it), so segment creation would 500 on it.
export function buildInsightsProjectSkipReason(deletedAt: string): string {
  return `its LFX Insights project was deleted on ${deletedAt.slice(0, 10)}, so the CDP team has to look at it`
}

export function needsDeletedProjectCheck(row: Pick<IDbProjectCatalog, 'proposedAction'>): boolean {
  return !joinsExistingProject(row)
}
