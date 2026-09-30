// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { isLiveDocs } from './http'

const VERSION_SEGMENT = /^(v\d+(\.\d+)*|\d+\.\d+(\.\d+)*|latest|stable|main|master|next)$/i
export const DOCS_SEGMENT = /^(docs?|documentation|guides?|manual|handbook|reference|learn)$/i
// readthedocs-style leading language folder (en, pt-br, zh_CN) that must stay with its version.
const LOCALE_SEGMENT = /^[a-z]{2}([-_][a-z]{2,4})?$/i
const GITHUB_HOSTS = new Set(['github.com', 'www.github.com'])
// Hosts where the first path segment names the project or space, so a cut must keep it.
const PATH_TENANT_SUFFIXES = ['github.io', 'gitbook.io']

// Cuts a versioned deep page back to its docs root, or returns null when nothing should be cut.
export function cutAtVersion(raw: string): string | null {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return null
  }
  if (GITHUB_HOSTS.has(url.hostname.toLowerCase())) {
    return null
  }

  const segments = url.pathname.split('/').filter(Boolean)
  const versionAt = segments.findIndex((segment) => VERSION_SEGMENT.test(segment))
  // Cutting must never drop a docs section that sits at or below the version segment.
  if (versionAt === -1 || segments.slice(versionAt).some((segment) => DOCS_SEGMENT.test(segment))) {
    return null
  }

  const withLocale = versionAt === 1 && LOCALE_SEGMENT.test(segments[0])
  const kept = segments.slice(0, withLocale ? versionAt + 1 : versionAt)
  if (kept.length === segments.length && !url.search && !url.hash) {
    return null
  }

  url.search = ''
  url.hash = ''
  url.pathname = kept.length === 0 ? '/' : `/${kept.join('/')}${withLocale ? '/' : ''}`
  return url.toString()
}

// The cut URL replaces the original only when it is itself a live docs page.
export async function cutToDocsRoot(url: string): Promise<string> {
  const cut = cutAtVersion(url)
  return cut && (await isLiveDocs(cut)) ? cut : url
}

// A deep SERP page cut back to its docs section, or to the host root when it has none.
export function cutAtDocsSegment(raw: string): string | null {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return null
  }

  const segments = url.pathname.split('/').filter(Boolean)
  const docsAt = segments.findIndex((segment) => DOCS_SEGMENT.test(segment))
  const host = url.hostname.toLowerCase()
  const keepsProjectSegment =
    docsAt === -1 && PATH_TENANT_SUFFIXES.some((suffix) => host.endsWith(`.${suffix}`))
  const kept = segments.slice(0, keepsProjectSegment ? 1 : docsAt + 1)
  const original = url.toString()

  url.search = ''
  url.hash = ''
  url.pathname = kept.length === 0 ? '/' : `/${kept.join('/')}`
  return url.toString() === original ? null : url.toString()
}

export async function cutSerpToDocsRoot(url: string): Promise<string> {
  const cut = cutAtDocsSegment(url)
  return cut && (await isLiveDocs(cut)) ? cut : url
}
