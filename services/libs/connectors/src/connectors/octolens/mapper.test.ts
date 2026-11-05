import { describe, expect, it } from 'vitest'

import { MemberIdentityType, PlatformType } from '@crowd/types'

import {
  mapMentionToActivity,
  octolensActivitySchema,
  profileLinkIdentity,
  toIsoTimestamp,
} from './mapper'
import type { OctolensMention } from './mentionsClient'

const BASE_MENTION: OctolensMention = {
  id: 200162933,
  sourceId: 'IC_kwDOA1b2c3',
  url: 'https://github.com/CrowdDotDev/crowd.dev/issues/12#issuecomment-1',
  title: 'Something about crowd.dev',
  body: 'post body',
  source: 'github',
  timestamp: '2026-10-12 08:30:00.000',
  author: 'octocat',
  authorName: 'The Octocat',
  authorUrl: 'https://github.com/octocat',
  authorAvatar: 'https://github.com/octocat.png',
  authorFollowers: 1200,
  relevance: 'relevant',
  relevanceComment: 'mentions the project by name',
  sentiment: 'Positive',
  language: 'english',
  tags: ['user_feedback'],
  keywords: [{ id: 42, keyword: 'crowd.dev', keywordTag: 'own_brand' }],
  engaged: false,
  isReply: true,
  keywordId: 42,
}

describe('mapMentionToActivity', () => {
  it('maps a github mention to a verified identity taken from the profile link', () => {
    const mapped = mapMentionToActivity(BASE_MENTION)

    expect(mapped?.platform).toBe(PlatformType.GITHUB)
    expect(mapped).toMatchObject({
      type: 'mention',
      platform: PlatformType.GITHUB,
      sourceId: '200162933',
      score: 0,
      channel: 'https://github.com/CrowdDotDev/crowd.dev',
      title: 'Something about crowd.dev',
      body: 'post body',
      url: 'https://github.com/CrowdDotDev/crowd.dev/issues/12#issuecomment-1',
      timestamp: '2026-10-12T08:30:00.000Z',
      member: {
        displayName: 'The Octocat',
        identities: [
          {
            platform: PlatformType.GITHUB,
            value: 'octocat',
            type: MemberIdentityType.USERNAME,
            verified: true,
          },
        ],
        attributes: {
          url: { [PlatformType.GITHUB]: 'https://github.com/octocat' },
          avatarUrl: { [PlatformType.GITHUB]: 'https://github.com/octocat.png' },
        },
      },
    })
  })

  it.each([
    [
      'reddit',
      'https://www.reddit.com/r/embedded/comments/1x9xiw3/zephyr_roadmap/',
      'https://www.reddit.com/user/someone',
      'embedded',
    ],
    ['github', 'https://github.com/settings/profile', 'https://github.com/octocat', 'crowd.dev'],
    ['twitter', 'https://x.com/someone/status/1', 'https://x.com/someone', 'crowd.dev'],
  ])('sets the %s mention channel to %s from %s', (source, url, authorUrl, channel) => {
    expect(mapMentionToActivity({ ...BASE_MENTION, source, url, authorUrl })?.channel).toBe(channel)
  })

  it('caps the channel at 150 characters', () => {
    const keyword = 'k'.repeat(200)
    const mapped = mapMentionToActivity({
      ...BASE_MENTION,
      source: 'twitter',
      url: 'https://x.com/someone/status/1',
      authorUrl: 'https://x.com/someone',
      keywords: [{ id: 42, keyword }],
    })

    expect(mapped?.channel).toBe('k'.repeat(150))
  })

  it('caps the channel by code point, never splitting an emoji', () => {
    const mapped = mapMentionToActivity({
      ...BASE_MENTION,
      source: 'twitter',
      url: 'https://x.com/someone/status/1',
      authorUrl: 'https://x.com/someone',
      keywords: [{ id: 42, keyword: `${'k'.repeat(149)}🚀🚀` }],
    })

    expect(mapped?.channel).toBe(`${'k'.repeat(149)}🚀`)
  })

  it('takes the linkedin handle from the profile link, not the display name in author', () => {
    const mapped = mapMentionToActivity({
      ...BASE_MENTION,
      source: 'linkedin',
      author: 'Daniel Kornaś',
      authorName: 'Daniel Kornaś',
      authorUrl: 'https://www.linkedin.com/in/daniel-kornas',
    })

    expect(mapped?.platform).toBe(PlatformType.LINKEDIN)
    expect(mapped?.member.identities[0].value).toBe('daniel-kornas')
    expect(mapped?.member.displayName).toBe('Daniel Kornaś')
    expect(mapped?.channel).toBe('crowd.dev')
  })

  it.each(['https://twitter.com/DanKornas', 'https://x.com/DanKornas'])(
    'takes the twitter handle from %s',
    (authorUrl) => {
      const mapped = mapMentionToActivity({ ...BASE_MENTION, source: 'twitter', authorUrl })

      expect(mapped?.platform).toBe(PlatformType.TWITTER)
      expect(mapped?.member.identities[0].value).toBe('DanKornas')
    },
  )

  it.each([
    ['a github app (bot)', 'https://github.com/apps/github-actions'],
    ['a linkedin company page', 'https://www.linkedin.com/company/crowd-dev'],
    ['a profile link with no handle', 'https://github.com/'],
  ])('skips %s', (_name, authorUrl) => {
    expect(mapMentionToActivity({ ...BASE_MENTION, authorUrl })).toBeNull()
  })

  it.each([
    ['reddit', 'https://github.com/octocat'],
    ['podcasts', 'https://twitter.com/someshow'],
    ['github', 'https://x.com/octocat'],
  ])(
    'skips a %s mention whose profile link points at another platform (%s)',
    (source, authorUrl) => {
      expect(mapMentionToActivity({ ...BASE_MENTION, source, authorUrl })).toBeNull()
    },
  )

  it.each([undefined, null, ''])('skips a mention without an Octolens id (%s)', (id) => {
    expect(
      mapMentionToActivity({ ...BASE_MENTION, id } as unknown as typeof BASE_MENTION),
    ).toBeNull()
  })

  it.each(['github', 'twitter', 'linkedin'])(
    'skips a %s mention without a profile link instead of trusting author',
    (source) => {
      expect(mapMentionToActivity({ ...BASE_MENTION, source, authorUrl: null })).toBeNull()
    },
  )

  it.each([
    ['reddit', 'https://www.reddit.com/user/SuperUranus', PlatformType.REDDIT, 'SuperUranus'],
    ['reddit_comment', 'https://old.reddit.com/u/someone', PlatformType.REDDIT, 'someone'],
    ['dev', 'https://dev.to/nughes', PlatformType.DEVTO, 'nughes'],
    [
      'hackernews',
      'https://news.ycombinator.com/user?id=AnthonyMouse',
      PlatformType.HACKERNEWS,
      'AnthonyMouse',
    ],
    [
      'bluesky',
      'https://bsky.app/profile/cerestial.bsky.social',
      PlatformType.BLUESKY,
      'cerestial.bsky.social',
    ],
    [
      'tiktok',
      'https://www.tiktok.com/@indahpermata04632',
      PlatformType.TIKTOK,
      'indahpermata04632',
    ],
    [
      'instagram',
      'https://www.instagram.com/techfairyanna/',
      PlatformType.INSTAGRAM,
      'techfairyanna',
    ],
    [
      'youtube',
      'https://www.youtube.com/channel/UC1axavgir413w00rwBHPAtQ',
      PlatformType.YOUTUBE,
      'UC1axavgir413w00rwBHPAtQ',
    ],
  ])(
    'emits a %s mention with the handle from its profile link',
    (source, authorUrl, platform, handle) => {
      const mapped = mapMentionToActivity({
        ...BASE_MENTION,
        source,
        authorUrl,
        author: 'Display Name',
      })

      expect(mapped?.platform).toBe(platform)
      expect(mapped?.channel).toBe(
        platform === PlatformType.GITHUB ? 'https://github.com/CrowdDotDev/crowd.dev' : 'crowd.dev',
      )
      expect(mapped?.member.identities[0]).toEqual({
        platform,
        value: handle,
        type: MemberIdentityType.USERNAME,
        verified: true,
      })
    },
  )

  it.each([
    ['reddit', 'https://www.reddit.com/user/[deleted]'],
    ['reddit', 'https://www.reddit.com/r/kubernetes'],
    ['dev', 'https://dev.to/t/kubernetes'],
    ['hackernews', 'https://news.ycombinator.com/item?id=50101892'],
    ['bluesky', 'https://bsky.app/search?q=x'],
    ['tiktok', 'https://www.tiktok.com/tag/kubernetes'],
    ['instagram', 'https://www.instagram.com/p/DefkRhFsgpL/'],
    ['youtube', 'https://www.youtube.com/@somehandle'],
    ['instagram', 'https://www.tiktok.com/@someone'],
  ])('skips a %s mention whose link %s is not a profile on that platform', (source, authorUrl) => {
    expect(mapMentionToActivity({ ...BASE_MENTION, source, authorUrl })).toBeNull()
  })

  it.each([
    ['stackoverflow', 'https://stackoverflow.com/users/1/someone'],
    ['podcasts', 'https://www.spreaker.com/show/alacarta'],
    ['news', null],
    ['firehose', null],
  ])('does not emit %s mentions', (source, authorUrl) => {
    expect(mapMentionToActivity({ ...BASE_MENTION, source, authorUrl })).toBeNull()
  })

  it('keeps the octolens fields and the mention context in activity.attributes', () => {
    const mapped = mapMentionToActivity({
      ...BASE_MENTION,
      relevanceScore: 0,
      feedbackRelevant: 1,
      engagementMetrics: { likes: 10, replies: 2 },
    })

    expect(mapped?.attributes).toEqual({
      octolensMentionId: 200162933,
      octolensSource: 'github',
      postId: 'IC_kwDOA1b2c3',
      keyword: 'crowd.dev',
      keywordId: 42,
      repoUrl: 'https://github.com/CrowdDotDev/crowd.dev',
      authorFollowers: 1200,
      relevance: 'relevant',
      relevanceScore: 0,
      relevanceComment: 'mentions the project by name',
      sentiment: 'Positive',
      language: 'english',
      tags: ['user_feedback'],
      keywords: [{ id: 42, keyword: 'crowd.dev', keywordTag: 'own_brand' }],
      engaged: false,
      isReply: true,
      feedbackRelevant: 1,
      engagementMetrics: { likes: 10, replies: 2 },
    })
  })

  it('records the keyword chosen by keywordId when the mention lists several', () => {
    const mapped = mapMentionToActivity({
      ...BASE_MENTION,
      keywords: [
        { id: 7, keyword: 'other' },
        { id: 42, keyword: 'crowd.dev' },
      ],
      keywordId: 42,
    })

    expect(mapped?.attributes).toMatchObject({ keyword: 'crowd.dev', keywordId: 42 })
  })

  it.each([
    ['https://github.com/octocat', undefined],
    ['https://github.com/orgs/CrowdDotDev/discussions/5', undefined],
    ['https://example.com/post', undefined],
    ['https://www.github.com/owner/repo.git', 'https://github.com/owner/repo'],
  ])('sets repoUrl for github url %s only when it names a repo', (url, repoUrl) => {
    const mapped = mapMentionToActivity({ ...BASE_MENTION, url })

    expect(mapped?.attributes?.repoUrl).toBe(repoUrl)
  })

  it('omits null optional fields instead of writing them as null', () => {
    const mapped = mapMentionToActivity({
      ...BASE_MENTION,
      authorFollowers: null,
      relevanceComment: null,
      sentiment: null,
      language: null,
    })

    const attributes = mapped?.attributes ?? {}
    for (const key of ['authorFollowers', 'relevanceComment', 'sentiment', 'language', 'review']) {
      expect(attributes).not.toHaveProperty(key)
    }
  })

  it('falls back to author, then the handle, for the display name', () => {
    expect(mapMentionToActivity({ ...BASE_MENTION, authorName: null })?.member.displayName).toBe(
      'octocat',
    )
    expect(
      mapMentionToActivity({ ...BASE_MENTION, authorName: '', author: '' })?.member.displayName,
    ).toBe('octocat')
  })

  it('passes through octolensActivitySchema unchanged', () => {
    const mapped = mapMentionToActivity(BASE_MENTION)

    expect(octolensActivitySchema.parse(mapped)).toEqual(mapped)
  })
})

describe('profileLinkIdentity', () => {
  it.each([
    ['https://github.com/octocat', { platform: PlatformType.GITHUB, value: 'octocat' }],
    ['https://x.com/someone', { platform: PlatformType.TWITTER, value: 'someone' }],
    ['https://uk.linkedin.com/in/someone', { platform: PlatformType.LINKEDIN, value: 'someone' }],
    ['https://github.com/apps/dependabot', null],
    ['https://www.linkedin.com/in/alice%2Fbob', null],
    ['https://www.linkedin.com/in/alice%5Cbob', null],
    ['https://mobile.twitter.com/someone', { platform: PlatformType.TWITTER, value: 'someone' }],
    ['https://x.com/i/user/123', null],
    ['https://x.com/compose/post', null],
    ['https://twitter.com/login', null],
    ['https://x.com/signup', null],
    ['https://github.com/user-', null],
    ['https://github.com/user--name', null],
    ['https://github.com/user-name', { platform: PlatformType.GITHUB, value: 'user-name' }],
    ['https://twitter.com/intent/user?screen_name=foo', null],
    ['https://twitter.com/hashtag/kubernetes', null],
    [
      'https://www.linkedin.com/in/j%C3%B6rg-x/',
      { platform: PlatformType.LINKEDIN, value: 'jörg-x' },
    ],
    ['https://www.reddit.com/user/someone', { platform: PlatformType.REDDIT, value: 'someone' }],
    ['https://example.com/someone', undefined],
    [null, undefined],
  ])('reads %s', (link, expected) => {
    expect(profileLinkIdentity(link)).toEqual(expected)
  })
})

describe('toIsoTimestamp', () => {
  it('converts the documented format to ISO8601 UTC', () => {
    expect(toIsoTimestamp('2026-10-12 08:30:00.000')).toBe('2026-10-12T08:30:00.000Z')
  })

  it('rejects a string that does not match the documented format', () => {
    expect(() => toIsoTimestamp('not-a-timestamp')).toThrow()
  })

  it('rejects an invalid calendar date instead of letting Date silently normalize it', () => {
    // Date would otherwise roll 2026-02-30 forward to 2026-03-02, moving the watermark
    // past valid mentions in between and permanently skipping them.
    expect(() => toIsoTimestamp('2026-02-30 08:00:00.000')).toThrow()
  })

  it('rejects an out-of-range time component instead of letting Date roll it over', () => {
    expect(() => toIsoTimestamp('2026-10-12 24:00:00.000')).toThrow()
  })
})
