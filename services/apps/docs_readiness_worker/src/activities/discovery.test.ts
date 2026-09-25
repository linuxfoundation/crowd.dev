import { afterEach, describe, expect, test, vi } from 'vitest'

import { resolveDocsUrl } from './discovery'

const mocks = vi.hoisted(() => ({
  findActiveProjectDocOverride: vi.fn(),
  findProjectForDocsDiscovery: vi.fn(),
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
  findEnabledRepositoriesForProject: mocks.findEnabledRepositoriesForProject,
  upsertProjectDocDiscovery: mocks.upsertProjectDocDiscovery,
}))

vi.mock('@crowd/common_services', () => ({
  getGithubInstallationToken: mocks.getGithubInstallationToken,
}))

vi.mock('../discovery', () => ({
  discoverDocs: mocks.discoverDocs,
}))

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

    expect(mocks.getGithubInstallationToken).not.toHaveBeenCalled()
    expect(mocks.discoverDocs).toHaveBeenCalledWith({
      name: 'Project',
      website: 'https://example.com',
      repos: [],
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
    })
    mocks.findEnabledRepositoriesForProject.mockResolvedValue([
      { url: 'https://github.com/org/repo' },
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
      website: null,
      repos: ['https://github.com/org/repo'],
      githubToken: 'gh-token',
      serpApiKey: 'serp-key',
    })
  })

  test('rejects if discoverDocs exceeds the discovery timeout, without upserting anything', async () => {
    vi.useFakeTimers()
    mocks.findActiveProjectDocOverride.mockResolvedValue(null)
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: 'https://example.com',
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
