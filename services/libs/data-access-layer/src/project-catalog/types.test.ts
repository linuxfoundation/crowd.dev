import { describe, expect, it } from 'vitest'

import {
  isGithubDiscussionProvenance,
  isReviewAlertProvenance,
  isSlackBotProvenance,
  isSlackPermalink,
} from './types'

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

describe('isSlackBotProvenance', () => {
  it('is true only for slack-bot provenance', () => {
    expect(isSlackBotProvenance('slack-bot')).toBe(true)
    expect(isSlackBotProvenance('slack-tag')).toBe(false)
    expect(isSlackBotProvenance('github-discussion')).toBe(false)
    expect(isSlackBotProvenance(null)).toBe(false)
  })
})

describe('isReviewAlertProvenance', () => {
  it('is true for github-discussion and slack-bot', () => {
    expect(isReviewAlertProvenance('github-discussion')).toBe(true)
    expect(isReviewAlertProvenance('slack-bot')).toBe(true)
  })

  it('is false for other provenances and null', () => {
    expect(isReviewAlertProvenance('slack-tag')).toBe(false)
    expect(isReviewAlertProvenance('lf-criticality-score')).toBe(false)
    expect(isReviewAlertProvenance(null)).toBe(false)
  })
})

describe('isSlackPermalink', () => {
  it('accepts workspace message permalinks', () => {
    expect(isSlackPermalink('https://acme.slack.com/archives/C1/p1700000000000100')).toBe(true)
  })

  it('rejects other urls', () => {
    expect(isSlackPermalink('https://github.com/foo/bar/discussions/1')).toBe(false)
    expect(isSlackPermalink('https://slack.com/archives/C1/p1')).toBe(false)
  })
})
