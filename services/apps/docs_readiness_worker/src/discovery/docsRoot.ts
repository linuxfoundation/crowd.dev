// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { isLiveDocs } from './http'

const VERSION_SEGMENT = /^(v?\d+(\.\d+)*|latest|stable|main|master|next|dev)$/i
const DOCS_SEGMENT = /^(docs?|documentation|guides?|manual|handbook|reference|learn)$/i
// readthedocs-style leading language folder (en, pt-br, zh_CN) that must stay with its version.
const LOCALE_SEGMENT = /^[a-z]{2}([-_][a-z]{2,4})?$/i
const GITHUB_HOSTS = new Set(['github.com', 'www.github.com'])

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
