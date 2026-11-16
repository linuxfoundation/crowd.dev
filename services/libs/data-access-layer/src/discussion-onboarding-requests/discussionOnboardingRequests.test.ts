import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  claimDiscussionOnboardingRequestDone,
  findDiscussionOnboardingRequest,
  replaceDiscussionOnboardingRequestComment,
  setDiscussionOnboardingRequestNotes,
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

    expect(stored).toMatchObject({ sourceUrl, discussionId: 'D_1', commentId: 'DC_1', notes: [] })
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

  test('replacing a deleted comment keeps the done notice', async ({ qx }) => {
    await upsertDiscussionOnboardingRequest(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_1',
    })
    await claimDiscussionOnboardingRequestDone(qx, sourceUrl)

    const replaced = await replaceDiscussionOnboardingRequestComment(qx, sourceUrl, 'DC_2')

    expect(replaced.commentId).toBe('DC_2')
    expect(replaced.doneNotifiedAt).not.toBeNull()
    expect(await claimDiscussionOnboardingRequestDone(qx, sourceUrl)).toBe(false)
  })

  test('keeps the notes the bot adds under the repositories', async ({ qx }) => {
    const note = '**github.com/acme** has no public repositories I can add.'
    const stored = await upsertDiscussionOnboardingRequest(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_1',
      notes: [note],
    })
    expect(stored.notes).toEqual([note])

    const cleared = await setDiscussionOnboardingRequestNotes(qx, sourceUrl, [])
    expect(cleared.notes).toEqual([])
    expect((await findDiscussionOnboardingRequest(qx, sourceUrl))?.notes).toEqual([])
  })
})
