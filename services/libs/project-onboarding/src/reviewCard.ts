import { IPublicRepoMetrics } from '@crowd/common'
import { fetchManySegments } from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { IReviewOrganization, IReviewRepoContext, IReviewRequestCard } from '@crowd/slack'

import {
  ReviewCase,
  buildCdpAdminUrl,
  buildCdpSegmentUrl,
  describeReviewCase,
  technicalDetails,
} from './botCopy'
import { sameProjectName } from './requestResolver'

export type ReviewCardRow = Pick<
  IDbProjectCatalog,
  | 'id'
  | 'repoName'
  | 'repoUrl'
  | 'projectName'
  | 'sourceUrl'
  | 'classificationKind'
  | 'proposedAction'
  | 'matchedProjectName'
  | 'matchedSegmentId'
  | 'evaluationReason'
  | 'evaluationResult'
>

type ReviewCaseRow = Pick<
  ReviewCardRow,
  'classificationKind' | 'proposedAction' | 'matchedSegmentId'
>
type ReviewCdpRow = Pick<ReviewCardRow, 'proposedAction' | 'matchedSegmentId'>

const ONBOARD_VERDICT = 'true'

// A new project waits only for auto-apply when the evaluator actually said onboard; an undecided
// verdict goes to review regardless of the switch.
function isAutoApplyHold(row: Pick<ReviewCardRow, 'proposedAction' | 'evaluationResult'>): boolean {
  switch (row.proposedAction) {
    case 'create_integration':
    case 'update_integration':
      return true
    case 'create_segment':
      return row.evaluationResult === ONBOARD_VERDICT
    default:
      return false
  }
}

export function needsPerson(row: Pick<ReviewCardRow, 'proposedAction'>): boolean {
  return row.proposedAction === 'human_review' || row.proposedAction === null
}

export function resolveReviewCase(row: ReviewCaseRow): ReviewCase {
  switch (row.proposedAction) {
    case 'create_segment':
      return 'new_project'
    case 'update_integration':
      return 'add_to_connection'
    case 'create_integration':
      return 'connect_github'
    case 'human_review':
      if (row.classificationKind === 'lf_not_in_cdp') {
        return 'project_not_set_up'
      }
      if (row.classificationKind === 'lf_not_in_pcc') {
        return 'not_in_pcc'
      }
      if (row.classificationKind === 'non_lf_new_project') {
        return 'new_project_by_hand'
      }
      if (row.classificationKind === 'ambiguous') {
        return 'handed_off'
      }
      return row.matchedSegmentId ? 'legacy_connection' : 'owner_only_match'
    default:
      return 'unknown'
  }
}

function namesDiffer(requested: string | null, matched: string | null): boolean {
  if (!requested || !matched) {
    return false
  }
  return !sameProjectName(requested, matched)
}

export interface IReviewCardExtras {
  cdpUrl: string | null
  repoContext: IReviewRepoContext | null
}

export const NO_REVIEW_CARD_EXTRAS: IReviewCardExtras = { cdpUrl: null, repoContext: null }

export function toReviewRepoContext(metrics: IPublicRepoMetrics): IReviewRepoContext {
  return {
    description: metrics.description,
    stars: metrics.stars,
    pushedAt: metrics.pushedAt,
    isArchived: metrics.isArchived,
    isFork: metrics.isFork,
    parentFullName: metrics.parentFullName,
  }
}

function reviewCaseProjectName(
  reviewCase: ReviewCase,
  row: Pick<ReviewCardRow, 'projectName' | 'matchedProjectName'>,
): string | null {
  return reviewCase === 'new_project' ||
    reviewCase === 'new_project_by_hand' ||
    reviewCase === 'not_in_pcc'
    ? row.projectName
    : row.matchedProjectName
}

export function toReviewRequestCard(
  row: ReviewCardRow,
  { cdpUrl, repoContext }: IReviewCardExtras,
  organization: IReviewOrganization | null = null,
): IReviewRequestCard {
  const reviewCase = resolveReviewCase(row)
  return {
    id: row.id,
    repoName: row.repoName,
    repoUrl: row.repoUrl,
    sourceUrl: row.sourceUrl,
    organization,
    repoContext,
    requestedName: row.projectName,
    matchedName: row.matchedProjectName,
    mismatch: namesDiffer(row.projectName, row.matchedProjectName),
    ...describeReviewCase(
      reviewCase,
      reviewCaseProjectName(reviewCase, row),
      organization?.repositories ?? 1,
    ),
    evaluationNote: reviewCase === 'new_project' ? row.evaluationReason : null,
    autoApplyHold: isAutoApplyHold(row),
    cdpUrl,
    details: technicalDetails(row),
  }
}

export async function resolveReviewCdpUrl(
  qx: QueryExecutor,
  row: ReviewCdpRow,
  frontendUrl: string | undefined,
): Promise<string | null> {
  if (!frontendUrl || !needsPerson(row)) {
    return null
  }
  if (!row.matchedSegmentId) {
    return buildCdpAdminUrl(frontendUrl)
  }

  const [segment] = await fetchManySegments(qx, [row.matchedSegmentId], 'id, "grandparentId"')
  return segment?.grandparentId
    ? buildCdpSegmentUrl(frontendUrl, segment.grandparentId, row.matchedSegmentId)
    : buildCdpAdminUrl(frontendUrl)
}
