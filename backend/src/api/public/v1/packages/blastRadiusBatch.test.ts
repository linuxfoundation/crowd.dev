import { describe, expect, it } from 'vitest'

import {
  MAX_BLAST_RADIUS_JOBS_PER_BATCH,
  MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH,
  blastRadiusJobBatchRequestSchema,
  blastRadiusJobPollBatchRequestSchema,
  paginateAnalysisIds,
} from './blastRadiusBatch'

describe('blastRadiusJobBatchRequestSchema', () => {
  it('accepts a batch of valid job requests', () => {
    const result = blastRadiusJobBatchRequestSchema.safeParse({
      jobs: [
        { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
        { advisoryId: 'CVE-2024-12345', ecosystem: 'npm', package: 'pkg:npm/lodash' },
      ],
    })
    expect(result.success).toBe(true)
  })

  it('rejects an empty jobs array', () => {
    const result = blastRadiusJobBatchRequestSchema.safeParse({ jobs: [] })
    expect(result.success).toBe(false)
  })

  it('rejects more than MAX_BLAST_RADIUS_JOBS_PER_BATCH jobs', () => {
    const jobs = Array.from({ length: MAX_BLAST_RADIUS_JOBS_PER_BATCH + 1 }, () => ({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
    }))
    const result = blastRadiusJobBatchRequestSchema.safeParse({ jobs })
    expect(result.success).toBe(false)
  })

  it('rejects a batch containing one invalid job', () => {
    const result = blastRadiusJobBatchRequestSchema.safeParse({
      jobs: [
        { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
        { advisoryId: 'not-an-advisory-id', ecosystem: 'npm' },
      ],
    })
    expect(result.success).toBe(false)
  })
})

describe('blastRadiusJobPollBatchRequestSchema', () => {
  const validId = '3fa85f64-5717-4562-b3fc-2c963f66afa6'

  it('accepts a batch of valid analysisIds and defaults page/pageSize', () => {
    const result = blastRadiusJobPollBatchRequestSchema.parse({ analysisIds: [validId] })
    expect(result.page).toBe(1)
    expect(result.pageSize).toBe(20)
  })

  it('rejects an empty analysisIds array', () => {
    const result = blastRadiusJobPollBatchRequestSchema.safeParse({ analysisIds: [] })
    expect(result.success).toBe(false)
  })

  it('rejects a non-uuid analysisId', () => {
    const result = blastRadiusJobPollBatchRequestSchema.safeParse({ analysisIds: ['not-a-uuid'] })
    expect(result.success).toBe(false)
  })

  it('rejects more than MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH analysisIds', () => {
    const analysisIds = Array.from(
      { length: MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH + 1 },
      () => validId,
    )
    const result = blastRadiusJobPollBatchRequestSchema.safeParse({ analysisIds })
    expect(result.success).toBe(false)
  })
})

describe('paginateAnalysisIds', () => {
  it('slices the requested page out of the full analysisIds array', () => {
    const analysisIds = ['a', 'b', 'c', 'd', 'e']
    const result = paginateAnalysisIds({ analysisIds, page: 2, pageSize: 2 })
    expect(result).toEqual({
      page: 2,
      pageSize: 2,
      total: 5,
      pagedAnalysisIds: ['c', 'd'],
    })
  })

  it('returns an empty page past the end of the array', () => {
    const result = paginateAnalysisIds({ analysisIds: ['a'], page: 2, pageSize: 20 })
    expect(result.pagedAnalysisIds).toEqual([])
    expect(result.total).toBe(1)
  })
})
