// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { afterEach, describe, expect, it, vi } from 'vitest'

import { cutAtVersion, cutToDocsRoot } from './docsRoot'

const httpMocks = vi.hoisted(() => ({ isLiveDocs: vi.fn<(url: string) => Promise<boolean>>() }))

vi.mock('./http', async () => ({
  ...(await vi.importActual<typeof import('./http')>('./http')),
  isLiveDocs: httpMocks.isLiveDocs,
}))

afterEach(() => {
  httpMocks.isLiveDocs.mockReset()
})

describe('cutAtVersion', () => {
  it.each([
    // Cut at the version segment and drop everything after it.
    [
      'zot',
      'https://zotregistry.dev/v2.1.20/install-guides/install-guide-k8s/',
      'https://zotregistry.dev/',
    ],
    ['v-prefixed', 'https://example.org/v3/api/thing', 'https://example.org/'],
    ['latest', 'https://example.org/latest/api', 'https://example.org/'],
    ['stable', 'https://example.org/stable/api', 'https://example.org/'],
    ['main', 'https://example.org/main/api', 'https://example.org/'],
    ['master', 'https://example.org/master/api', 'https://example.org/'],
    ['next', 'https://example.org/next/api', 'https://example.org/'],
    ['query and hash are dropped', 'https://example.org/v2/x?a=1#top', 'https://example.org/'],
    [
      'what precedes the version is kept',
      'https://example.org/project/v2/x',
      'https://example.org/project',
    ],
    // A docs-like segment before the version is kept.
    ['docs above the version', 'https://example.org/docs/v2/x', 'https://example.org/docs'],
    [
      'documentation above the version',
      'https://example.org/documentation/latest/x',
      'https://example.org/documentation',
    ],
    // readthedocs-style locale + version stays valid.
    [
      'readthedocs locale',
      'https://rez.readthedocs.io/en/stable/commands/rez-help.html',
      'https://rez.readthedocs.io/en/stable/',
    ],
    [
      'readthedocs numeric version',
      'https://warpx.readthedocs.io/en/26.02/usage/workflows.html',
      'https://warpx.readthedocs.io/en/26.02/',
    ],
    [
      'readthedocs a/b',
      'https://x.readthedocs.io/en/latest/a/b',
      'https://x.readthedocs.io/en/latest/',
    ],
    [
      'regional locale',
      'https://x.readthedocs.io/pt-br/latest/a',
      'https://x.readthedocs.io/pt-br/latest/',
    ],
  ])('%s: cuts %s', (_name, url, expected) => {
    expect(cutAtVersion(url)).toBe(expected)
  })

  it.each([
    ['no version segment', 'https://docs.example.com/docs/getting-started'],
    ['a docs page without a version', 'https://docs.example.com/guide/install'],
    ['root url', 'https://docs.example.com/'],
    ['a docs segment below the version', 'https://example.org/v2/docs/intro'],
    ['a guide below the version', 'https://example.org/latest/guides/intro'],
    [
      'a version-like word inside a longer segment',
      'https://example.org/install-guides/install-guide-k8s',
    ],
    ['already at locale + version', 'https://x.readthedocs.io/en/latest/'],
    [
      'a stable-kali style segment',
      'https://cntt.readthedocs.io/en/stable-kali/gov/chapters/chapter01.html',
    ],
    ['a GitHub repo tree', 'https://github.com/acme/proj/tree/main/docs'],
    ['a bare number such as a blog year', 'https://example.org/blog/2024/05/post'],
    ['a dev section', 'https://example.org/dev/api'],
    ['not a url', 'not a url'],
  ])('%s: leaves %s untouched', (_name, url) => {
    expect(cutAtVersion(url)).toBeNull()
  })
})

describe('cutToDocsRoot', () => {
  const deep = 'https://zotregistry.dev/v2.1.20/install-guides/install-guide-k8s/'

  it('returns the cut url when it is live', async () => {
    httpMocks.isLiveDocs.mockResolvedValue(true)

    expect(await cutToDocsRoot(deep)).toBe('https://zotregistry.dev/')
    expect(httpMocks.isLiveDocs).toHaveBeenCalledWith('https://zotregistry.dev/')
  })

  it('keeps the original url when the cut url is not live', async () => {
    httpMocks.isLiveDocs.mockResolvedValue(false)

    expect(await cutToDocsRoot(deep)).toBe(deep)
  })

  it('does not probe anything when there is no version segment', async () => {
    const url = 'https://docs.example.com/docs/getting-started'

    expect(await cutToDocsRoot(url)).toBe(url)
    expect(httpMocks.isLiveDocs).not.toHaveBeenCalled()
  })
})
