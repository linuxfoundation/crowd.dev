// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { registrableDomain } from '@crowd/common'

import { normalizedDomain } from './http'
import { nameTokens } from './relevance'

// Foundation sites that host many unrelated projects; sharing one is always an umbrella.
const UMBRELLA_SITES = [
  'aswf.io',
  'lfenergy.org',
  'openssf.org',
  'lfedge.org',
  'landscape.finos.org',
  'linuxfoundation.org',
  'lfaidata.foundation',
  'openmainframeproject.org',
  'lfnetworking.org',
  'hyperledger.org',
  'lfdecentralizedtrust.org',
]

// Foundation sites whose bare home page names the foundation, never one of its projects.
const FOUNDATION_SITES = [
  ...UMBRELLA_SITES,
  'chipsalliance.org',
  'cncf.io',
  'r-consortium.org',
  'greensoftware.foundation',
  'interledger.org',
  'openstxfoundation.org',
]

// Name words that add no identity; a fund shares the docs site of its project.
const CORPORATE_WORDS = ['inc']
const EDITION_WORDS = ['fund', 'initiative']

const MIN_FIRST_TOKEN_LENGTH = 3

interface ISharedWebsiteInput {
  name: string
  slug: string
  website: string | null
  siblings: { name: string; slug: string }[]
}

const tokenKey = (name: string): string => [...new Set(nameTokens(name))].sort().join(' ')

// True only for a site that hosts unrelated projects. A twin entry, a site named after this
// project, or a family sharing its first name word (ODL ...) is not an umbrella.
export function isUmbrellaWebsite({ name, website, siblings }: ISharedWebsiteInput): boolean {
  if (!website || siblings.length === 0) {
    return false
  }

  const host = normalizedDomain(website)
  if (host && UMBRELLA_SITES.some((site) => host === site || host.endsWith(`.${site}`))) {
    return true
  }

  const ownKey = tokenKey(name)
  const others = siblings.filter((sibling) => !ownKey || tokenKey(sibling.name) !== ownKey)
  if (others.length === 0) {
    return false
  }

  const label = registrableDomain(website)?.split('.')[0] ?? ''
  if (label && nameTokens(name).some((token) => label.includes(token))) {
    return false
  }

  const [firstToken] = nameTokens(name)
  const isFamily =
    !!firstToken &&
    firstToken.length >= MIN_FIRST_TOKEN_LENGTH &&
    others.some((sibling) => nameTokens(sibling.name)[0] === firstToken)
  return !isFamily
}

const compact = (value: string): string => value.toLowerCase().replace(/[^a-z0-9]/g, '')

function isSiteRoot(url: string): boolean {
  try {
    return new URL(url).pathname.replace(/\/+$/, '') === ''
  } catch {
    return false
  }
}

// The page host and path carry every name word (minus the ignored ones) or the bracketed alias.
function namesProject(url: string, name: string, ignored: string[]): boolean {
  let target: string
  try {
    const { hostname, pathname } = new URL(url)
    target = compact(hostname + pathname)
  } catch {
    return false
  }
  const alias = /\(([^)]+)\)/.exec(name)?.[1]
  const tokens = nameTokens(name.replace(/\([^)]*\)/g, '')).filter(
    (token) => !ignored.includes(token),
  )
  return (
    (!!alias && target.includes(compact(alias))) ||
    tokens.every((token) => target.includes(compact(token)))
  )
}

// A docs URL other projects share, or a foundation home page, counts only for the project it names.
export function isUnnamedParentPage(url: string, name: string, isShared: boolean): boolean {
  const domain = normalizedDomain(url)
  const foundationHome = !!domain && FOUNDATION_SITES.includes(domain) && isSiteRoot(url)
  if (!foundationHome && !isShared) {
    return false
  }
  return !namesProject(
    url,
    name,
    foundationHome ? CORPORATE_WORDS : [...CORPORATE_WORDS, ...EDITION_WORDS],
  )
}
