import { z } from 'zod'

import { MemberIdentityType, PlatformType } from '@crowd/types'

import type { OctolensMention } from './mentionsClient'

// Only sources whose author is a person: podcasts, news, firehose, newsletter and review sites
// name a show or publication.
const SOURCE_TO_PLATFORM: Record<string, PlatformType> = {
  github: PlatformType.GITHUB,
  twitter: PlatformType.TWITTER,
  linkedin: PlatformType.LINKEDIN,
  reddit: PlatformType.REDDIT,
  reddit_comment: PlatformType.REDDIT,
  dev: PlatformType.DEVTO,
  hackernews: PlatformType.HACKERNEWS,
  stackoverflow: PlatformType.STACKOVERFLOW,
  medium: PlatformType.MEDIUM,
  producthunt: PlatformType.PRODUCTHUNT,
  facebook: PlatformType.FACEBOOK,
  youtube: PlatformType.YOUTUBE,
  bluesky: PlatformType.BLUESKY,
  tiktok: PlatformType.TIKTOK,
  instagram: PlatformType.INSTAGRAM,
}

// Every source whose live mentions carry a verifiable profile link; stackoverflow, medium,
// producthunt and facebook stay mapped but off until Octolens actually sends them.
const ENABLED_PLATFORMS = new Set<PlatformType>([
  PlatformType.GITHUB,
  PlatformType.TWITTER,
  PlatformType.LINKEDIN,
  PlatformType.REDDIT,
  PlatformType.DEVTO,
  PlatformType.HACKERNEWS,
  PlatformType.BLUESKY,
  PlatformType.TIKTOK,
  PlatformType.INSTAGRAM,
  PlatformType.YOUTUBE,
])

const GITHUB_NON_OWNER_PATHS = new Set([
  'about',
  'account',
  'advisories',
  'apps',
  'codespaces',
  'collections',
  'copilot',
  'customer-stories',
  'dashboard',
  'enterprise',
  'events',
  'explore',
  'features',
  'issues',
  'login',
  'marketplace',
  'new',
  'notifications',
  'organizations',
  'orgs',
  'pricing',
  'pulls',
  'readme',
  'resources',
  'search',
  'security',
  'settings',
  'site',
  'solutions',
  'sponsors',
  'stars',
  'topics',
  'trending',
  'users',
])

// Mentions are for identity enrichment only, so they never move a member's engagement score.
const OCTOLENS_MENTION_SCORE = 0
const MAX_CHANNEL_LENGTH = 150

const identitySchema = z.object({
  platform: z.string(),
  value: z.string(),
  type: z.string(),
  verified: z.boolean(),
})

const memberSchema = z.object({
  displayName: z.string(),
  identities: z.array(identitySchema).min(1),
  attributes: z.record(z.unknown()).optional(),
})

export const octolensActivitySchema = z.object({
  type: z.literal('mention'),
  platform: z.string(),
  timestamp: z.string(),
  sourceId: z.string(),
  score: z.number(),
  title: z.string().optional(),
  body: z.string().optional(),
  url: z.string().optional(),
  channel: z.string().optional(),
  attributes: z.record(z.unknown()).optional(),
  member: memberSchema,
})

export type OctolensActivity = z.infer<typeof octolensActivitySchema>

export function mapMentionToActivity(mention: OctolensMention): OctolensActivity | null {
  const author = authorIdentity(mention)
  if (!author || !hasMentionId(mention.id)) {
    return null
  }
  const { platform, value: identityValue } = author

  // Each attribute name is a top-level key, wrapped per-platform underneath — matches the
  // GitHub connector's own memberAttributesSchema shape, not { [platform]: { url, ... } }.
  const attributes: Record<string, unknown> = {}
  if (mention.authorUrl) attributes.url = { [platform]: mention.authorUrl }
  if (mention.authorAvatar) attributes.avatarUrl = { [platform]: mention.authorAvatar }

  // authorFollowers stays here, not on member.reach: Crustdata enrichment already writes
  // reach.linkedin from connections, and these per-mention snapshots can arrive out of order.
  const keyword = matchedKeyword(mention)
  const repoUrl = platform === PlatformType.GITHUB ? parseGithubRepoUrl(mention.url) : undefined
  const subreddit = platform === PlatformType.REDDIT ? parseSubreddit(mention.url) : undefined
  const activityAttributes = withoutNullish({
    octolensMentionId: mention.id,
    octolensSource: mention.source,
    postId: mention.sourceId,
    keyword,
    repoUrl,
    subreddit,
    authorFollowers: mention.authorFollowers,
    relevance: mention.relevance,
    relevanceScore: mention.relevanceScore,
    relevanceComment: mention.relevanceComment,
    sentiment: mention.sentiment,
    language: mention.language,
    tags: mention.tags,
    keywords: mention.keywords,
    keywordId: mention.keywordId,
    engaged: mention.engaged,
    isReply: mention.isReply,
    feedbackRelevant: mention.feedbackRelevant,
    imageUrl: mention.imageUrl,
    engagementMetrics: mention.engagementMetrics,
    engagementObservedAt: mention.engagementObservedAt,
    review: mention.review ? withoutNullish({ ...mention.review }) : undefined,
  })

  return {
    type: 'mention',
    platform,
    timestamp: toIsoTimestamp(mention.timestamp),
    sourceId: String(mention.id),
    score: OCTOLENS_MENTION_SCORE,
    title: mention.title ?? undefined,
    body: mention.body ?? undefined,
    url: mention.url,
    channel: mentionChannel({ repoUrl, subreddit, keyword, source: mention.source }),
    attributes: Object.keys(activityAttributes).length > 0 ? activityAttributes : undefined,
    member: {
      displayName: mention.authorName || mention.author || identityValue,
      identities: [
        {
          platform,
          value: identityValue,
          type: MemberIdentityType.USERNAME,
          verified: true,
        },
      ],
      attributes: Object.keys(attributes).length > 0 ? attributes : undefined,
    },
  }
}

interface AuthorIdentity {
  platform: PlatformType
  value: string
}

// Identity comes from the author's profile link on the mention's own platform: Octolens's
// `author` is a display name on LinkedIn, and a github.com/apps/ link is a bot, not a person.
function authorIdentity(mention: OctolensMention): AuthorIdentity | null {
  const platform = SOURCE_TO_PLATFORM[mention.source.toLowerCase()]
  if (!platform || !ENABLED_PLATFORMS.has(platform)) return null

  const fromLink = profileLinkIdentity(mention.authorUrl)
  return fromLink && fromLink.platform === platform ? fromLink : null
}

const GITHUB_LOGIN_RE = /^(?=.{1,39}$)[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$/
const TWITTER_HANDLE_RE = /^[A-Za-z0-9_]{1,15}$/
const TWITTER_NON_HANDLE_PATHS = new Set([
  'compose',
  'explore',
  'hashtag',
  'home',
  'i',
  'intent',
  'login',
  'logout',
  'messages',
  'notifications',
  'search',
  'settings',
  'share',
  'signup',
  'tos',
])

// Reddit's `[deleted]` fails REDDIT_USERNAME_RE, so deleted accounts never share one identity.
const REDDIT_USERNAME_RE = /^[A-Za-z0-9_-]{3,20}$/
const DEVTO_USERNAME_RE = /^[A-Za-z0-9_]{1,30}$/
const DEVTO_NON_USER_PATHS = new Set([
  'about',
  'code-of-conduct',
  'enter',
  'faq',
  'latest',
  'privacy',
  'search',
  'settings',
  't',
  'terms',
  'top',
])
const HACKERNEWS_USERNAME_RE = /^[A-Za-z0-9_-]{2,15}$/
const BLUESKY_HANDLE_RE = /^(?:did:[a-z]+:[A-Za-z0-9._:-]+|(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})$/
const TIKTOK_HANDLE_RE = /^@([A-Za-z0-9_.]{2,24})$/
const INSTAGRAM_USERNAME_RE = /^[A-Za-z0-9_.]{1,30}$/
const INSTAGRAM_NON_USER_PATHS = new Set([
  'accounts',
  'direct',
  'explore',
  'p',
  'reel',
  'reels',
  'stories',
  'tv',
])
const YOUTUBE_CHANNEL_ID_RE = /^UC[A-Za-z0-9_-]{22}$/

// undefined: not a known profile link; null: a known host, but no usable person.
export function profileLinkIdentity(
  raw: string | null | undefined,
): AuthorIdentity | null | undefined {
  const url = parseUrl(raw)
  if (!url) return undefined
  const host = url.hostname.toLowerCase().replace(/^(www|mobile|m|old|new)\./, '')
  const segments = pathSegments(url).map(decodeSegment)

  if (host === 'github.com') {
    const [login] = segments
    return login && GITHUB_LOGIN_RE.test(login) && !GITHUB_NON_OWNER_PATHS.has(login.toLowerCase())
      ? { platform: PlatformType.GITHUB, value: login }
      : null
  }
  if (host === 'twitter.com' || host === 'x.com') {
    const [handle] = segments
    return handle &&
      TWITTER_HANDLE_RE.test(handle) &&
      !TWITTER_NON_HANDLE_PATHS.has(handle.toLowerCase())
      ? { platform: PlatformType.TWITTER, value: handle }
      : null
  }
  if (host === 'linkedin.com' || host.endsWith('.linkedin.com')) {
    const [kind, handle] = segments
    return kind === 'in' && handle && !/[\\/]/.test(handle)
      ? { platform: PlatformType.LINKEDIN, value: handle }
      : null
  }
  if (host === 'reddit.com' || host.endsWith('.reddit.com')) {
    const [kind, username] = segments
    return (kind === 'user' || kind === 'u') && username && REDDIT_USERNAME_RE.test(username)
      ? { platform: PlatformType.REDDIT, value: username }
      : null
  }
  if (host === 'dev.to') {
    const [username] = segments
    return username &&
      DEVTO_USERNAME_RE.test(username) &&
      !DEVTO_NON_USER_PATHS.has(username.toLowerCase())
      ? { platform: PlatformType.DEVTO, value: username }
      : null
  }
  if (host === 'news.ycombinator.com') {
    const username = url.searchParams.get('id')
    return segments[0] === 'user' && username && HACKERNEWS_USERNAME_RE.test(username)
      ? { platform: PlatformType.HACKERNEWS, value: username }
      : null
  }
  if (host === 'bsky.app') {
    const [kind, handle] = segments
    return kind === 'profile' && handle && BLUESKY_HANDLE_RE.test(handle)
      ? { platform: PlatformType.BLUESKY, value: handle }
      : null
  }
  if (host === 'tiktok.com') {
    const match = segments[0]?.match(TIKTOK_HANDLE_RE)
    return match ? { platform: PlatformType.TIKTOK, value: match[1] } : null
  }
  if (host === 'instagram.com') {
    const [username] = segments
    return username &&
      INSTAGRAM_USERNAME_RE.test(username) &&
      !INSTAGRAM_NON_USER_PATHS.has(username.toLowerCase())
      ? { platform: PlatformType.INSTAGRAM, value: username }
      : null
  }
  if (host === 'youtube.com') {
    const [kind, channelId] = segments
    return kind === 'channel' && channelId && YOUTUBE_CHANNEL_ID_RE.test(channelId)
      ? { platform: PlatformType.YOUTUBE, value: channelId }
      : null
  }
  return undefined
}

function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment)
  } catch {
    return segment
  }
}

function hasMentionId(id: unknown): boolean {
  return Number.isSafeInteger(id) || (typeof id === 'string' && id.length > 0)
}

// Like the native integrations: the repo URL for GitHub, the subreddit for Reddit; platforms with
// no community concept use the matched keyword. Capped to activityRelations.channel's varchar(150).
function mentionChannel(parts: {
  repoUrl?: string
  subreddit?: string
  keyword?: string
  source: string
}): string {
  const channel = parts.repoUrl ?? parts.subreddit ?? parts.keyword ?? parts.source
  // By code point, as varchar counts them; slicing UTF-16 units could leave half a surrogate pair.
  return Array.from(channel).slice(0, MAX_CHANNEL_LENGTH).join('')
}

function matchedKeyword(mention: OctolensMention): string | undefined {
  const keywords = Array.isArray(mention.keywords) ? mention.keywords : []
  const match = keywords.find((k) => k?.id === mention.keywordId)
  const text = typeof match?.keyword === 'string' ? match.keyword.trim() : ''
  return text || undefined
}

function parseUrl(raw: string | null | undefined): URL | null {
  if (!raw) return null
  try {
    return new URL(raw)
  } catch {
    return null
  }
}

function pathSegments(url: URL): string[] {
  return url.pathname.split('/').filter((segment) => segment.length > 0)
}

export function parseGithubRepoUrl(raw: string | null | undefined): string | undefined {
  const url = parseUrl(raw)
  if (!url || !['github.com', 'www.github.com'].includes(url.hostname.toLowerCase())) {
    return undefined
  }
  const [owner, rawRepo] = pathSegments(url)
  const repo = rawRepo?.replace(/\.git$/i, '')
  if (!owner || !repo || GITHUB_NON_OWNER_PATHS.has(owner.toLowerCase())) {
    return undefined
  }
  return `https://github.com/${owner}/${repo}`
}

export function parseSubreddit(raw: string | null | undefined): string | undefined {
  const url = parseUrl(raw)
  if (!url || !/(^|\.)reddit\.com$/i.test(url.hostname)) return undefined
  const [first, subreddit] = pathSegments(url)
  return first?.toLowerCase() === 'r' && subreddit ? subreddit : undefined
}

function withoutNullish(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== null && v !== undefined))
}

const OCTOLENS_TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/

// Octolens returns "YYYY-MM-DD HH:mm:ss.SSS", UTC with no timezone suffix (confirmed
// against the live API docs) — append Z before parsing so Date treats it as UTC, not local.
export function toIsoTimestamp(raw: string): string {
  if (!OCTOLENS_TIMESTAMP_RE.test(raw)) {
    throw new Error(`octolens timestamp does not match the documented format: ${raw}`)
  }
  const iso = `${raw.replace(' ', 'T')}Z`
  const parsed = new Date(iso)
  // Date silently normalizes an invalid calendar date/time (e.g. 2026-02-30 -> Mar 2), so
  // verify the round trip matches instead of trusting a seemingly successful parse.
  if (parsed.toISOString() !== iso) {
    throw new Error(`octolens timestamp is not a valid calendar date/time: ${raw}`)
  }
  return iso
}
