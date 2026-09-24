import { describe, expect, it } from 'vitest'

import { isGithubDiscussionProvenance } from './types'

describe('isGithubDiscussionProvenance', () => {
  it('is true for github-discussion', () => {
    expect(isGithubDiscussionProvenance('github-discussion')).toBe(true)
  })

  it('is false for slack-tag', () => {
    expect(isGithubDiscussionProvenance('slack-tag')).toBe(false)
  })

  it('is false for lf-criticality-score', () => {
    expect(isGithubDiscussionProvenance('lf-criticality-score')).toBe(false)
  })

  it('is false for null', () => {
    expect(isGithubDiscussionProvenance(null)).toBe(false)
  })
})
