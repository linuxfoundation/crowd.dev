import { canonicalizeRepoUrl } from '@crowd/common'
import {
  IDbProjectCatalog,
  ProjectCatalogAction,
  ProjectEvaluationOutcome,
} from '@crowd/data-access-layer/src/project-catalog/types'
import {
  OrganizationGroupRow,
  organizationGroupKey,
  organizationOf,
  sameReviewProposal,
} from '@crowd/project-onboarding'

export interface ISharedEvaluation {
  action: ProjectCatalogAction
  evaluationResult: string
  evaluationReason: string | null
  reviewReason: string | null
}

const SHARED_ACTIONS = new Set<ProjectCatalogAction>(['review', 'onboard', 'skip'])
const ONBOARD_VERDICT = 'true'

type EvaluatedSibling = IDbProjectCatalog & { evaluationResult: string }

function hasSharedVerdict(
  row: IDbProjectCatalog,
  lead: IDbProjectCatalog,
): row is EvaluatedSibling {
  return (
    sameReviewProposal(row, lead) &&
    row.evaluatedAt !== null &&
    row.evaluationResult !== null &&
    SHARED_ACTIONS.has(row.action)
  )
}

type MainRepositoryRow = Pick<IDbProjectCatalog, 'repoUrl' | 'requestHints'>

export function isOrganizationMainRepository(row: MainRepositoryRow): boolean {
  const owner = organizationOf(row)
  const repo = canonicalizeRepoUrl(row.repoUrl)?.repo
  return !!owner && !!repo && repo.toLowerCase() === owner.toLowerCase()
}

// The first evaluated row of a group decides for the rest, so the main repository goes first
// and is the one the evaluator reads.
export function organizationMainRepositoriesFirst<
  T extends OrganizationGroupRow & MainRepositoryRow,
>(rows: T[]): T[] {
  const firstIndexByKey = new Map<string, number>()
  const ordered = [...rows]
  rows.forEach((row, index) => {
    const key = organizationGroupKey(row)
    if (!key) {
      return
    }
    const first = firstIndexByKey.get(key)
    if (first === undefined) {
      firstIndexByKey.set(key, index)
      return
    }
    if (isOrganizationMainRepository(row) && !isOrganizationMainRepository(ordered[first])) {
      ;[ordered[first], ordered[index]] = [ordered[index], ordered[first]]
    }
  })
  return ordered
}

export function sharedOrganizationEvaluation(
  lead: IDbProjectCatalog,
  siblings: IDbProjectCatalog[],
): ISharedEvaluation | null {
  const source = siblings.find((row): row is EvaluatedSibling => hasSharedVerdict(row, lead))
  if (!source) {
    return null
  }
  return {
    action: source.action,
    evaluationResult: source.evaluationResult,
    evaluationReason: source.evaluationReason,
    reviewReason: source.reviewReason,
  }
}

export function sharedEvaluationOutcome(shared: ISharedEvaluation): ProjectEvaluationOutcome {
  if (shared.action === 'skip') {
    return 'skip'
  }
  return shared.evaluationResult === ONBOARD_VERDICT ? 'onboard' : 'unsure'
}
