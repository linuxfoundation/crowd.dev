import { PlatformType } from '@crowd/types'

export type SocialPlatform = PlatformType.GITHUB | PlatformType.LINKEDIN | PlatformType.TWITTER

const GITHUB_HANDLE = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$/
const GITHUB_RESERVED = new Set([
  'about',
  'apps',
  'collections',
  'enterprise',
  'explore',
  'features',
  'login',
  'marketplace',
  'orgs',
  'settings',
  'sponsors',
  'topics',
])
const LINKEDIN_HANDLE = /^[\p{L}\p{N}_-]{3,100}$/u
const X_HANDLE = /^[A-Za-z0-9_]{1,15}$/
const X_RESERVED = new Set(['hashtag', 'home', 'i', 'intent', 'login', 'search', 'share'])

function parseUrl(link: string): URL | null {
  try {
    return new URL(/^https?:\/\//i.test(link) ? link : `https://${link}`)
  } catch {
    return null
  }
}

function decode(part: string): string {
  try {
    return decodeURIComponent(part)
  } catch {
    return part
  }
}

export function socialProfile(link: string): { platform: SocialPlatform; handle: string } | null {
  const url = parseUrl(link.trim())
  if (!url) {
    return null
  }
  const host = url.hostname.toLowerCase().replace(/^(www|mobile|[a-z]{2})\./, '')
  const parts = url.pathname.split('/').filter(Boolean).map(decode)

  if (host === 'github.com' && parts[0]) {
    const handle = parts[0]
    return GITHUB_HANDLE.test(handle) && !GITHUB_RESERVED.has(handle.toLowerCase())
      ? { platform: PlatformType.GITHUB, handle }
      : null
  }
  if (host === 'linkedin.com' && parts[0] === 'in' && parts[1]) {
    const handle = parts[1]
    return LINKEDIN_HANDLE.test(handle) ? { platform: PlatformType.LINKEDIN, handle } : null
  }
  if ((host === 'twitter.com' || host === 'x.com') && parts[0]) {
    const handle = parts[0].replace(/^@/, '')
    return X_HANDLE.test(handle) && !X_RESERVED.has(handle.toLowerCase())
      ? { platform: PlatformType.TWITTER, handle }
      : null
  }
  return null
}

function linksIn(text: string): string[] {
  const links: string[] = []
  for (const match of text.matchAll(/href=["']([^"']+)["']/gi)) {
    links.push(match[1])
  }
  for (const match of text.matchAll(
    /(?<=^|[\s(>"'[])(?:https?:\/\/)?(?:[a-z]{2,6}\.)?(?:github\.com|linkedin\.com|twitter\.com|x\.com)\/[^\s"'<>),]+/gi,
  )) {
    links.push(match[0].replace(/[.;:!?]+$/, ''))
  }
  return links
}

export function socialHandles(
  website: string | undefined,
  about: string | undefined,
): Partial<Record<SocialPlatform, string>> {
  const handles: Partial<Record<SocialPlatform, string>> = {}
  const links = [...(website ? [website] : []), ...(about ? linksIn(about) : [])]
  for (const link of links) {
    const profile = socialProfile(link)
    if (profile && !handles[profile.platform]) {
      handles[profile.platform] = profile.handle
    }
  }
  return handles
}
