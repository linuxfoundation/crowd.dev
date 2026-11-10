import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  claimDiscussionOnboardingRequestDone,
  findDiscussionOnboardingRequest,
  upsertDiscussionOnboardingRequest,
} from './discussionOnboardingRequests'

const test = withQx(base)

const sourceUrl = 'https://github.com/linuxfoundation/insights/discussions/2496'

describe('discussionOnboardingRequests', () => {
  test('stores the status comment of a discussion and reads it back by its source url', async ({
    qx,
  }) => {
    const stored = await upsertDiscussionOnboardingRequest(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_1',
    })

    expect(stored).toMatchObject({ sourceUrl, discussionId: 'D_1', commentId: 'DC_1' })
    expect(stored.doneNotifiedAt).toBeNull()
    expect(await findDiscussionOnboardingRequest(qx, sourceUrl)).toMatchObject({
      commentId: 'DC_1',
    })
    expect(
      await findDiscussionOnboardingRequest(
        qx,
        'https://github.com/linuxfoundation/insights/discussions/1',
      ),
    ).toBeNull()
  })

  test('a re-run replaces the comment and reopens the done notice', async ({ qx }) => {
    await upsertDiscussionOnboardingRequest(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_1',
    })
    expect(await claimDiscussionOnboardingRequestDone(qx, sourceUrl)).toBe(true)
    expect(await claimDiscussionOnboardingRequestDone(qx, sourceUrl)).toBe(false)

    const replaced = await upsertDiscussionOnboardingRequest(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_2',
    })

    expect(replaced.commentId).toBe('DC_2')
    expect(await claimDiscussionOnboardingRequestDone(qx, sourceUrl)).toBe(true)
  })
})
