import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { resolveDocsUrl } from './discovery'

const mocks = vi.hoisted(() => ({
  findActiveProjectDocOverride: vi.fn(),
  findProjectForDocsDiscovery: vi.fn(),
  findSharedDocsUrls: vi.fn(),
  findEnabledRepositoriesForProject: vi.fn(),
  upsertProjectDocDiscovery: vi.fn(),
  getGithubInstallationToken: vi.fn(),
  discoverDocs: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: {
      reader: { connection: () => ({}) },
      writer: { connection: () => ({}) },
    },
  },
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  pgpQx: vi.fn(() => ({})),
}))

vi.mock('@crowd/data-access-layer', () => ({
  findActiveProjectDocOverride: mocks.findActiveProjectDocOverride,
  findProjectForDocsDiscovery: mocks.findProjectForDocsDiscovery,
  findSharedDocsUrls: mocks.findSharedDocsUrls,
  findEnabledRepositoriesForProject: mocks.findEnabledRepositoriesForProject,
  upsertProjectDocDiscovery: mocks.upsertProjectDocDiscovery,
}))

vi.mock('@crowd/common_services', () => ({
  getGithubInstallationToken: mocks.getGithubInstallationToken,
}))

vi.mock('../discovery', () => ({
  discoverDocs: mocks.discoverDocs,
}))

beforeEach(() => {
  mocks.findSharedDocsUrls.mockResolvedValue([])
})

afterEach(() => {
  vi.clearAllMocks()
  delete process.env.CROWD_DOCS_READINESS_SERP_API_KEY
})

describe('resolveDocsUrl', () => {
  test('short-circuits to the active override without running discovery', async () => {
    mocks.findActiveProjectDocOverride.mockResolvedValue({
      docsUrl: 'https://override.example.com/docs',
    })

    const result = await resolveDocsUrl('project-1')

    expect(result).toEqual({
      docsUrl: 'https://override.example.com/docs',
      discoveryMethod: 'override',
      confidence: 'authoritative',
      isOverride: true,
    })
    expect(mocks.discoverDocs).not.toHaveBeenCalled()
    expect(mocks.upsertProjectDocDiscovery).not.toHaveBeenCalled()
  })

  test('throws a non-retryable failure when the project does not exist', async () => {
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue(null)

    await expect(resolveDocsUrl('missing-project')).rejects.toThrow()
    expect(mocks.discoverDocs).not.toHaveBeenCalled()
  })

  test('runs discovery, upserts the result, and does not fetch a GitHub token when there are no repos', async () => {
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: 'https://example.com',
      websiteSharedWith: [],
    })
    mocks.findEnabledRepositoriesForProject.mockResolvedValue([])
    mocks.discoverDocs.mockResolvedValue({
      docsUrl: 'https://docs.example.com',
      discoveryMethod: 'docs-subdomain',
      confidence: 'high',
      allCandidates: [
        {
          url: 'https://docs.example.com',
          method: 'docs-subdomain',
          confidence: 'high',
          livenessOk: true,
        },
      ],
    })

    const result = await resolveDocsUrl('project-1')

    expect(mocks.findProjectForDocsDiscovery).toHaveBeenCalledWith({}, 'project-1', {
      withWebsiteSharedWith: true,
    })
    expect(mocks.getGithubInstallationToken).not.toHaveBeenCalled()
    expect(mocks.discoverDocs).toHaveBeenCalledWith({
      name: 'Project',
      slug: 'proj',
      website: 'https://example.com',
      websiteShared: false,
      websiteSharedByFamily: false,
      repos: [],
      findSharedDocsUrls: expect.any(Function),
      githubToken: null,
      serpApiKey: null,
    })
    expect(mocks.upsertProjectDocDiscovery).toHaveBeenCalledWith(
      {},
      expect.objectContaining({
        projectId: 'project-1',
        docsUrl: 'https://docs.example.com',
        discoveryMethod: 'docs-subdomain',
        confidence: 'high',
      }),
    )
    expect(result).toEqual({
      docsUrl: 'https://docs.example.com',
      discoveryMethod: 'docs-subdomain',
      confidence: 'high',
      isOverride: false,
    })
  })

  test('fetches a GitHub token and passes the SERP key when repos and the env var exist', async () => {
    process.env.CROWD_DOCS_READINESS_SERP_API_KEY = 'serp-key'
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: null,
      websiteSharedWith: [],
    })
    mocks.findEnabledRepositoriesForProject.mockResolvedValue([
      { url: 'https://github.com/org/repo', starCount: 42 },
    ])
    mocks.getGithubInstallationToken.mockResolvedValue('gh-token')
    mocks.discoverDocs.mockResolvedValue({
      docsUrl: null,
      discoveryMethod: null,
      confidence: null,
      allCandidates: [],
    })

    await resolveDocsUrl('project-1')

    expect(mocks.getGithubInstallationToken).toHaveBeenCalledTimes(1)
    expect(mocks.discoverDocs).toHaveBeenCalledWith({
      name: 'Project',
      slug: 'proj',
      website: null,
      websiteShared: false,
      websiteSharedByFamily: false,
      repos: [{ url: 'https://github.com/org/repo', starCount: 42 }],
      findSharedDocsUrls: expect.any(Function),
      githubToken: 'gh-token',
      serpApiKey: 'serp-key',
    })
  })

  test('scopes the shared docs URL lookup to this project and the given hosts', async () => {
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: null,
      websiteSharedWith: [],
    })
    mocks.findEnabledRepositoriesForProject.mockResolvedValue([])
    mocks.findSharedDocsUrls.mockResolvedValue(['https://foundation.org', 'https://www.aswf.io/'])
    mocks.discoverDocs.mockResolvedValue({
      docsUrl: null,
      discoveryMethod: null,
      confidence: null,
      allCandidates: [],
    })

    await resolveDocsUrl('project-1')

    expect(mocks.findSharedDocsUrls).not.toHaveBeenCalled()
    const lookup = mocks.discoverDocs.mock.calls[0][0].findSharedDocsUrls
    expect(await lookup(['foundation.org', 'aswf.io'])).toEqual([
      'https://foundation.org',
      'https://www.aswf.io/',
    ])
    expect(mocks.findSharedDocsUrls).toHaveBeenCalledWith({}, 'project-1', [
      'foundation.org',
      'aswf.io',
    ])
  })

  test.each([
    {
      label: 'Electron with its twin',
      name: 'Electron',
      slug: 'ojsf-electron',
      website: 'https://www.electronjs.org/',
      sharedWith: [{ name: 'Electron framework', slug: 'electron-electron' }],
      expected: { websiteShared: false, websiteSharedByFamily: true },
    },
    {
      label: 'ODL SAF with its family',
      name: 'ODL Service Abstraction Framework (SAF)',
      slug: 'odl-saf',
      website: 'https://www.opendaylight.org/',
      sharedWith: [
        { name: 'ODL Guice', slug: 'odl-guice' },
        { name: 'OpenDaylight', slug: 'opendaylight' },
      ],
      expected: { websiteShared: false, websiteSharedByFamily: true },
    },
    {
      label: 'Rez on the aswf.io umbrella',
      name: 'Rez',
      slug: 'rez',
      website: 'https://www.aswf.io/',
      sharedWith: [{ name: 'MaterialX', slug: 'materialx' }],
      expected: { websiteShared: true, websiteSharedByFamily: false },
    },
    {
      label: 'a unique website',
      name: 'Rez',
      slug: 'rez',
      website: 'https://rez.example.org/',
      sharedWith: [],
      expected: { websiteShared: false, websiteSharedByFamily: false },
    },
  ])('derives the website sharing flags for $label', async (c) => {
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: c.slug,
      name: c.name,
      website: c.website,
      websiteSharedWith: c.sharedWith,
    })
    mocks.findEnabledRepositoriesForProject.mockResolvedValue([])
    mocks.discoverDocs.mockResolvedValue({
      docsUrl: null,
      discoveryMethod: null,
      confidence: null,
      allCandidates: [],
    })

    await resolveDocsUrl('project-1')

    expect(mocks.discoverDocs).toHaveBeenCalledWith(expect.objectContaining(c.expected))
  })

  test('rejects if discoverDocs exceeds the discovery timeout, without upserting anything', async () => {
    vi.useFakeTimers()
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: 'https://example.com',
      websiteSharedWith: [],
    })
    mocks.findEnabledRepositoriesForProject.mockResolvedValue([])
    mocks.discoverDocs.mockReturnValue(new Promise(() => {}))

    const result = resolveDocsUrl('project-1')
    const assertion = expect(result).rejects.toThrow(/exceeded/)
    await vi.advanceTimersByTimeAsync(4 * 60 * 1000)
    await assertion

    expect(mocks.upsertProjectDocDiscovery).not.toHaveBeenCalled()
    vi.useRealTimers()
  })
})
