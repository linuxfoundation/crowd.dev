import { beforeEach, describe, expect, it, vi } from 'vitest'

const { countProjectCatalogOpenBySourceUrl, setSlackRequestReactions } = vi.hoisted(() => ({
  countProjectCatalogOpenBySourceUrl: vi.fn(),
  setSlackRequestReactions: vi.fn(),
}))
vi.mock('@crowd/data-access-layer', () => ({ countProjectCatalogOpenBySourceUrl }))
vi.mock('@crowd/slack', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/slack')>()),
  setSlackRequestReactions,
}))

import { applyRequestStatus, reactionsToRemove } from './botReactions'

const project = {
  id: 'row-1',
  repoUrl: 'https://github.com/acme/one',
  sourceUrl: 'https://acme.slack.com/archives/C1/p1760000000123456',
  requestHints: null,
}

describe('reactionsToRemove', () => {
  it('keeps the hourglass while sibling rows are still being processed', () => {
    expect(reactionsToRemove('added', { pending: 1, inReview: 0, failed: 0, asked: 0 })).toEqual([
      'eyes',
      'question',
    ])
    expect(reactionsToRemove('withTeam', { pending: 2, inReview: 1, failed: 0, asked: 1 })).toEqual(
      [],
    )
  })

  it('drops the hourglass once nothing is pending and the eyes once nothing is in review', () => {
    expect(reactionsToRemove('withTeam', { pending: 0, inReview: 1, failed: 0, asked: 1 })).toEqual(
      ['hourglass_flowing_sand'],
    )
    expect(reactionsToRemove('notAdded', { pending: 0, inReview: 1, failed: 0, asked: 1 })).toEqual(
      ['hourglass_flowing_sand'],
    )
    expect(reactionsToRemove('added', { pending: 0, inReview: 0, failed: 0, asked: 0 })).toEqual([
      'hourglass_flowing_sand',
      'eyes',
      'question',
    ])
    expect(reactionsToRemove('checking', { pending: 1, inReview: 0, failed: 0, asked: 0 })).toEqual(
      ['eyes', 'warning', 'question'],
    )
  })

  it('clears the warning on a retry only when no sibling row is still failed', () => {
    expect(reactionsToRemove('checking', { pending: 0, inReview: 1, failed: 0, asked: 1 })).toEqual(
      ['warning'],
    )
    expect(reactionsToRemove('checking', { pending: 0, inReview: 1, failed: 1, asked: 1 })).toEqual(
      [],
    )
    expect(reactionsToRemove('added', { pending: 0, inReview: 0, failed: 1, asked: 1 })).toEqual([
      'hourglass_flowing_sand',
      'eyes',
    ])
  })

  it('drops the not-added mark when a repo of an organization request gets added', () => {
    const open = { pending: 0, inReview: 0, failed: 0, asked: 0 }

    expect(reactionsToRemove('added', open, true)).toContain('no_entry_sign')
    expect(reactionsToRemove('added', open)).not.toContain('no_entry_sign')
    expect(reactionsToRemove('notAdded', open, true)).not.toContain('no_entry_sign')
  })

  it('keeps the question mark while a sibling row still waits for the requester', () => {
    expect(
      reactionsToRemove('needsInput', { pending: 0, inReview: 0, failed: 0, asked: 1 }),
    ).toEqual(['hourglass_flowing_sand', 'eyes'])
    expect(reactionsToRemove('checking', { pending: 1, inReview: 0, failed: 0, asked: 1 })).toEqual(
      ['eyes', 'warning'],
    )
  })
})

describe('applyRequestStatus', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    setSlackRequestReactions.mockResolvedValue({ ok: true })
  })

  it('adds the outcome reaction and removes the in-progress ones on the request message', async () => {
    countProjectCatalogOpenBySourceUrl.mockResolvedValue({
      pending: 0,
      inReview: 0,
      failed: 0,
      asked: 0,
    })

    await applyRequestStatus({} as never, project, 'notAdded')

    expect(countProjectCatalogOpenBySourceUrl).toHaveBeenCalledWith({}, project.sourceUrl)
    expect(setSlackRequestReactions).toHaveBeenCalledWith(project.sourceUrl, {
      add: ['no_entry_sign'],
      remove: ['hourglass_flowing_sand', 'eyes', 'question'],
    })
  })

  it('removes the not-added mark on the mention when an organization repo is added', async () => {
    countProjectCatalogOpenBySourceUrl.mockResolvedValue({
      pending: 0,
      inReview: 0,
      failed: 0,
      asked: 0,
    })

    await applyRequestStatus(
      {} as never,
      { ...project, requestHints: { organization: 'acme' } },
      'added',
    )

    expect(setSlackRequestReactions).toHaveBeenCalledWith(project.sourceUrl, {
      add: ['white_check_mark'],
      remove: ['no_entry_sign', 'hourglass_flowing_sand', 'eyes', 'question'],
    })
  })

  it('does nothing without a source url and never throws on failures', async () => {
    await applyRequestStatus({} as never, { ...project, sourceUrl: null }, 'added')
    expect(setSlackRequestReactions).not.toHaveBeenCalled()

    countProjectCatalogOpenBySourceUrl.mockRejectedValue(new Error('db down'))
    await expect(applyRequestStatus({} as never, project, 'added')).resolves.toBeUndefined()
  })
})
