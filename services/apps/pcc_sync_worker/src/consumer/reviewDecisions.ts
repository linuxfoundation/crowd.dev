import type { QueryExecutor } from '@crowd/database'

import type { ExistingProjectCandidate } from './existingProjects'

export type PccReviewDecision = 'create_new' | 'ignore'

export interface PccProjectReview {
  decision: PccReviewDecision
  reviewedSegmentIds: string[]
}

export type PccProjectReviews = Map<string, PccProjectReview>

export function isAlreadyReviewed(
  review: PccProjectReview | undefined,
  candidates: Pick<ExistingProjectCandidate, 'id'>[],
): boolean {
  if (!review) return false
  if (review.decision === 'ignore') return true
  const reviewedSegmentIds = new Set(review.reviewedSegmentIds)
  return candidates.every((candidate) => reviewedSegmentIds.has(candidate.id))
}

export async function loadPccProjectReviews(qx: QueryExecutor): Promise<PccProjectReviews> {
  const rows: {
    pccProjectId: string
    decision: PccReviewDecision
    reviewedSegmentIds: string[]
  }[] = await qx.select(
    `
      SELECT
        pcc_project_id AS "pccProjectId",
        decision,
        reviewed_segment_ids::text[] AS "reviewedSegmentIds"
      FROM pcc_project_review_decisions
      `,
  )
  return new Map(
    rows.map(({ pccProjectId, decision, reviewedSegmentIds }) => [
      pccProjectId,
      { decision, reviewedSegmentIds },
    ]),
  )
}
