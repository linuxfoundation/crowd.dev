// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { registrableDomain } from '@crowd/common'

import { normalizedDomain } from './http'
import { nameTokens, projectTokens } from './relevance'

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
]

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
export function isUmbrellaWebsite({ name, slug, website, siblings }: ISharedWebsiteInput): boolean {
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
  if (label && projectTokens({ name, slug }).some((token) => label.includes(token))) {
    return false
  }

  const [firstToken] = nameTokens(name)
  const isFamily =
    !!firstToken &&
    firstToken.length >= MIN_FIRST_TOKEN_LENGTH &&
    others.some((sibling) => nameTokens(sibling.name)[0] === firstToken)
  return !isFamily
}
