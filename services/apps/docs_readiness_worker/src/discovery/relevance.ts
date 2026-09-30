// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { registrableDomain } from '@crowd/common'

import { parseGithubRepo } from './github'

const GENERIC_WORDS = new Set([
  'project',
  'foundation',
  'framework',
  'the',
  'for',
  'docs',
  'documentation',
  'open',
  'source',
])

const MIN_TOKEN_LENGTH = 3
const MIN_AFFIX_TOKEN_LENGTH = 5
const MIN_HYPHEN_PART_TOKEN_LENGTH = 4

// Hosts that never carry a project's own documentation, whatever the query returned.
const NOISE_HOSTS = [
  'wikipedia.org',
  'instagram.com',
  'linkedin.com',
  'facebook.com',
  'twitter.com',
  'x.com',
  'youtube.com',
  'reddit.com',
  'medium.com',
  'stackoverflow.com',
  'docs.google.com',
  'github.com',
  'gitlab.com',
]

// One tenant per project, so the host alone says nothing: the tenant and first path segment do.
const SHARED_HOSTING = [
  'readthedocs.io',
  'github.io',
  'gitbook.io',
  'netlify.app',
  'vercel.app',
  'pages.dev',
  'docs.rs',
]

const words = (value: string): string[] =>
  value
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean)

const isOnDomain = (host: string, domain: string): boolean =>
  host === domain || host.endsWith(`.${domain}`)

// Name words minus generic ones ("Electron framework" -> ["electron"]).
export function nameTokens(name: string): string[] {
  return words(name).filter((word) => !GENERIC_WORDS.has(word))
}

interface IProjectTokenSource {
  name: string
  slug: string
  repoUrl?: string | null
}

// Distinctive lowercase words from the project name/slug and its repo owner/name.
export function projectTokens({ name, slug, repoUrl }: IProjectTokenSource): string[] {
  const repo = repoUrl ? parseGithubRepo(repoUrl) : null
  const wholeSlug = slug.toLowerCase().replace(/[^a-z0-9]/g, '')
  const all = [
    ...nameTokens(name),
    ...nameTokens(slug),
    ...(repo ? [...nameTokens(repo.owner), ...nameTokens(repo.repo)] : []),
  ]
  return [...new Set(all.filter((word) => word.length >= MIN_TOKEN_LENGTH || word === wholeSlug))]
}

// Short tokens must be the whole label, so "torq" never matches "qtorque".
// hyphenTokens (README links, name/slug only) may match a 4+ char hyphen part: besu -> besu-eth.
const matchesLabel = (label: string, token: string, hyphenTokens: string[]): boolean =>
  label === token ||
  (hyphenTokens.includes(token) &&
    token.length >= MIN_HYPHEN_PART_TOKEN_LENGTH &&
    label.split('-').includes(token)) ||
  (token.length >= MIN_AFFIX_TOKEN_LENGTH && (label.startsWith(token) || label.endsWith(token)))

export function isRelevantSerpResult(
  url: string,
  tokens: string[],
  { hyphenTokens = [] }: { hyphenTokens?: string[] } = {},
): boolean {
  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    return false
  }

  const host = parsed.hostname.toLowerCase().replace(/^www\./, '')
  if (NOISE_HOSTS.some((noise) => isOnDomain(host, noise))) {
    return false
  }

  const hosting = SHARED_HOSTING.find((suffix) => isOnDomain(host, suffix))
  const labels: string[] = []
  if (hosting) {
    const tenant =
      host
        .slice(0, -hosting.length - 1)
        .split('.')
        .pop() ?? ''
    const firstSegment = parsed.pathname.split('/').filter(Boolean)[0] ?? ''
    labels.push(tenant, firstSegment.toLowerCase())
  } else {
    // Only the registrable label counts: subdomains and the public suffix never match.
    const root = registrableDomain(host)
    if (root) {
      labels.push(root.split('.')[0])
    }
  }

  return tokens.some((token) => labels.some((label) => matchesLabel(label, token, hyphenTokens)))
}
