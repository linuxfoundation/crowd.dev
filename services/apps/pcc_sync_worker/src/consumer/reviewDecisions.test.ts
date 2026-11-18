import { test as base, describe, expect } from 'vitest'

import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { isAlreadyReviewed, loadPccProjectReviews } from './reviewDecisions'
import type { PccProjectReview } from './reviewDecisions'

const test = withQx(base)

const REVIEWED_SEGMENT_ID = '11111111-1111-4111-8111-111111111111'
const NEW_SEGMENT_ID = '22222222-2222-4222-8222-222222222222'

const differentProject: PccProjectReview = {
  decision: 'create_new',
  reviewedSegmentIds: [REVIEWED_SEGMENT_ID],
}

async function insertReview(
  qx: QueryExecutor,
  pccProjectId: string,
  decision: string,
  reviewedSegmentIds: string[],
): Promise<void> {
  await qx.result(
    `
    INSERT INTO pcc_project_review_decisions
      (pcc_project_id, decision, reviewed_segment_ids, decided_by)
    VALUES ($(pccProjectId), $(decision), $(reviewedSegmentIds)::uuid[], 'reviewer')
    `,
    { pccProjectId, decision, reviewedSegmentIds },
  )
}

describe('isAlreadyReviewed', () => {
  test('is false for a project nobody reviewed', () => {
    expect(isAlreadyReviewed(undefined, [{ id: REVIEWED_SEGMENT_ID }])).toBe(false)
  })

  test('is true when every similar CDP project was reviewed', () => {
    expect(isAlreadyReviewed(differentProject, [{ id: REVIEWED_SEGMENT_ID }])).toBe(true)
  })

  test('is false when a similar CDP project appeared after the review', () => {
    const candidates = [{ id: REVIEWED_SEGMENT_ID }, { id: NEW_SEGMENT_ID }]

    expect(isAlreadyReviewed(differentProject, candidates)).toBe(false)
  })

  test('is true for an ignored project whatever CDP project looks like it', () => {
    const ignored: PccProjectReview = { decision: 'ignore', reviewedSegmentIds: [] }

    expect(isAlreadyReviewed(ignored, [{ id: NEW_SEGMENT_ID }])).toBe(true)
  })
})

describe('loadPccProjectReviews', () => {
  test('returns each review by its PCC project id', async ({ qx }) => {
    await insertReview(qx, 'pcc-different', 'create_new', [REVIEWED_SEGMENT_ID])
    await insertReview(qx, 'pcc-test', 'ignore', [])

    const reviews = await loadPccProjectReviews(qx)

    expect(reviews.get('pcc-different')).toEqual(differentProject)
    expect(reviews.get('pcc-test')).toEqual({ decision: 'ignore', reviewedSegmentIds: [] })
  })

  test('returns nothing when no project was reviewed', async ({ qx }) => {
    expect((await loadPccProjectReviews(qx)).size).toBe(0)
  })
})
