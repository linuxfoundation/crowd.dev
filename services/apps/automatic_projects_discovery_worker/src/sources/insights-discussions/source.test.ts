import { describe, expect, it } from 'vitest'

import { InsightsDiscussionsSource } from './source'

describe('InsightsDiscussionsSource.parseRow', () => {
  const source = new InsightsDiscussionsSource()

  it('propagates discussionUrl as sourceUrl', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com/foo/bar',
      discussionUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
    })

    expect(result?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/42')
  })

  it('leaves sourceUrl undefined when discussionUrl is missing', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com/foo/bar',
    })

    expect(result?.sourceUrl).toBeUndefined()
  })

  it('returns null when repoUrl is missing', () => {
    const result = source.parseRow({
      discussionUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
    })

    expect(result).toBeNull()
  })

  it('returns null when repoUrl cannot be parsed into a project identity', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com',
      discussionUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
    })

    expect(result).toBeNull()
  })
})
