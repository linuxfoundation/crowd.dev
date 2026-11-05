import { beforeEach, describe, expect, it, vi } from 'vitest'

import * as jobsDal from '@crowd/data-access-layer/src/osspckgs/memberContributorJobs'

import { startInferredRepoMemberContributorsJob } from '../startInferredRepo'

const qx = vi.hoisted(() => ({}))
vi.mock('../../../db', () => ({ getPackagesDb: vi.fn().mockResolvedValue(qx) }))
vi.mock('@crowd/data-access-layer/src/osspckgs/memberContributorJobs', () => ({
  markMemberContributorJobRunning: vi.fn(),
  findMemberContributorJobPurl: vi.fn(),
}))

const markRunning = vi.mocked(jobsDal.markMemberContributorJobRunning)
const findPurl = vi.mocked(jobsDal.findMemberContributorJobPurl)

beforeEach(() => vi.clearAllMocks())

describe('startInferredRepoMemberContributorsJob', () => {
  it('marks the job running before reading its purl and ecosystem', async () => {
    findPurl.mockResolvedValue({ purl: 'pkg:hackage/aeson', ecosystem: 'hackage' })

    await expect(startInferredRepoMemberContributorsJob({ jobId: 'job-1' })).resolves.toEqual({
      purl: 'pkg:hackage/aeson',
      ecosystem: 'hackage',
    })
    expect(markRunning.mock.calls).toEqual([[qx, 'job-1']])
    expect(findPurl.mock.calls).toEqual([[qx, 'job-1']])
    expect(markRunning.mock.invocationCallOrder[0]).toBeLessThan(
      findPurl.mock.invocationCallOrder[0],
    )
  })

  it('fails without retry when the job row is missing', async () => {
    findPurl.mockResolvedValue(null)
    const error = await startInferredRepoMemberContributorsJob({ jobId: 'job-1' }).catch(
      (e: unknown) => e,
    )
    expect(error).toMatchObject({ message: 'Job not found', nonRetryable: true })
  })
})
