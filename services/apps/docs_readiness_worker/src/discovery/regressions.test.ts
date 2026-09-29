// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { afterEach, describe, expect, it, vi } from 'vitest'

import { discoverDocs } from './index'
import { isUmbrellaWebsite } from './sharedWebsite'
import type { IDiscoveryContext } from './strategies'

// End-to-end discovery over the real strategies with a scripted network: only listed URLs are live.
const HTML = { status: 200, headers: { 'content-type': 'text/html' } }
const stripSlash = (url: string) => url.replace(/\/+$/, '')

function scriptNetwork(live: string[], githubHomepages: Record<string, string> = {}) {
  const liveKeys = new Set(live.map(stripSlash))
  const fetchMock = vi.fn((input: string | URL | Request) => {
    const url = typeof input === 'string' ? input : input.toString()
    const repo = url.match(/^https:\/\/api\.github\.com\/repos\/([^/]+\/[^/]+)$/)?.[1]
    if (repo && githubHomepages[repo]) {
      return Promise.resolve(Response.json({ homepage: githubHomepages[repo] }))
    }
    if (!liveKeys.has(stripSlash(url))) {
      return Promise.reject(new Error(`no route for ${url}`))
    }
    const response = new Response('<html></html>', HTML)
    Object.defineProperty(response, 'url', { value: url })
    return Promise.resolve(response)
  })
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

const serpCalls = (fetchMock: ReturnType<typeof scriptNetwork>) =>
  fetchMock.mock.calls.filter(([url]) => String(url).startsWith('https://serpapi.com'))

interface IProject {
  name: string
  slug: string
  website: string | null
  sharedWith?: { name: string; slug: string }[]
  repos?: IDiscoveryContext['repos']
}

// Mirrors how resolveDocsUrl turns a project row into the discovery context.
function ctxFor(project: IProject, findSharedDocsUrls?: (hosts: string[]) => Promise<string[]>) {
  const siblings = project.sharedWith ?? []
  const umbrella = isUmbrellaWebsite({ ...project, siblings })
  return {
    name: project.name,
    slug: project.slug,
    website: project.website,
    websiteShared: umbrella,
    websiteSharedByFamily: siblings.length > 0 && !umbrella,
    repos: project.repos ?? [],
    githubToken: project.repos?.length ? 'token' : null,
    serpApiKey: 'serp-key',
    findSharedDocsUrls,
  } satisfies IDiscoveryContext
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('discovery regressions from the IN-1396 prod re-run', () => {
  it('Electron: the twin entry does not disable the website strategies, and SERP stays off', async () => {
    const fetchMock = scriptNetwork([
      'https://www.electronjs.org/',
      'https://www.electronjs.org/docs',
      'https://www.jenkins.io/doc/',
    ])

    const result = await discoverDocs(
      ctxFor({
        name: 'Electron',
        slug: 'ojsf-electron',
        website: 'https://www.electronjs.org/',
        sharedWith: [{ name: 'Electron framework', slug: 'electron-electron' }],
      }),
    )

    expect(result.docsUrl).toBe('https://www.electronjs.org/docs')
    expect(result.discoveryMethod).toBe('docs-path')
    expect(serpCalls(fetchMock)).toHaveLength(0)
  })

  it('5 Spot Machine Scheduler: the repo homepage wins and SERP is never consulted', async () => {
    const fetchMock = scriptNetwork(['https://5spot.finos.org/'], {
      'finos/5-spot': 'https://5spot.finos.org/',
    })

    const result = await discoverDocs(
      ctxFor({
        name: '5 Spot Machine Scheduler',
        slug: 'five-spot-machine-scheduler',
        website: null,
        repos: [{ url: 'https://github.com/finos/5-spot', starCount: 3 }],
      }),
    )

    expect(result.docsUrl).toBe('https://5spot.finos.org/')
    expect(result.discoveryMethod).toBe('github-homepage')
    expect(serpCalls(fetchMock)).toHaveLength(0)
  })

  const ODL_SIBLINGS = [
    { name: 'ODL Guice', slug: 'odl-guice' },
    { name: 'ODL Micro', slug: 'odl-micro' },
    { name: 'OpenDaylight', slug: 'opendaylight' },
  ]

  it.each([
    { name: 'ODL Service Abstraction Framework (SAF)', slug: 'odl-saf' },
    { name: 'ODL Guice', slug: 'odl-guice' },
  ])('$name keeps docs.opendaylight.org although the family claims it', async (project) => {
    scriptNetwork(['https://www.opendaylight.org/', 'https://docs.opendaylight.org'])

    const result = await discoverDocs(
      ctxFor(
        { ...project, website: 'https://www.opendaylight.org/', sharedWith: ODL_SIBLINGS },
        async () => ['https://docs.opendaylight.org'],
      ),
    )

    expect(result.docsUrl).toBe('https://docs.opendaylight.org')
    expect(result.discoveryMethod).toBe('docs-subdomain')
  })

  it('GraphQL IDE Monorepo keeps graphql.org/docs although its siblings claim it', async () => {
    scriptNetwork(['https://graphql.org/', 'https://graphql.org/docs'])

    const result = await discoverDocs(
      ctxFor(
        {
          name: 'GraphQL IDE Monorepo',
          slug: 'gql-language-service',
          website: 'https://graphql.org/',
          sharedWith: ['Express-GraphQL', 'GraphiQL', 'GraphQL.js'].map((name) => ({
            name,
            slug: name.toLowerCase(),
          })),
        },
        async () => ['https://graphql.org/docs'],
      ),
    )

    expect(result.docsUrl).toBe('https://graphql.org/docs')
    expect(result.discoveryMethod).toBe('docs-path')
  })

  it('a docs URL claimed by another project still loses when the site is not this family', async () => {
    scriptNetwork(['https://example.org/', 'https://docs.example.org'])

    const result = await discoverDocs(
      ctxFor({ name: 'Solo', slug: 'solo', website: 'https://example.org/' }, async () => [
        'https://docs.example.org',
      ]),
    )

    expect(result.docsUrl).toBe('https://example.org/')
  })

  it('a foundation umbrella site skips the website strategies entirely', async () => {
    const fetchMock = scriptNetwork(['https://www.aswf.io/', 'https://docs.aswf.io'])

    const result = await discoverDocs(
      ctxFor({
        name: 'Rez',
        slug: 'rez',
        website: 'https://www.aswf.io/',
        sharedWith: [{ name: 'MaterialX', slug: 'materialx' }],
      }),
    )

    expect(result.docsUrl).toBeNull()
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('aswf.io'))).toBe(false)
  })
})
