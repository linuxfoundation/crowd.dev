import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import type { IDocCandidate } from '@crowd/data-access-layer'

import { pickValidatedWinner } from '../discovery/pickValidation'
import { resolveDocsUrl } from './discovery'

const mocks = vi.hoisted(() => ({
  findActiveProjectDocOverride: vi.fn(),
  findProjectForDocsDiscovery: vi.fn(),
  findSharedDocsUrls: vi.fn(),
  findEnabledRepositoriesForProject: vi.fn(),
  upsertProjectDocDiscovery: vi.fn(),
  getGithubInstallationToken: vi.fn(),
  discoverDocs: vi.fn(),
  createClient: vi.fn((): import('../discovery/docsValidator').DocsValidatorClient => ({
    complete: vi.fn(),
  })),
  probe: vi.fn(),
  fetchText: vi.fn(),
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

vi.mock('../discovery/http', async () => {
  const actual = await vi.importActual<typeof import('../discovery/http')>('../discovery/http')
  return { ...actual, probe: mocks.probe, fetchText: mocks.fetchText }
})

vi.mock('../discovery/docsValidatorClient', () => ({
  createAnthropicAwsDocsValidatorClient: mocks.createClient,
}))

beforeEach(() => {
  mocks.findSharedDocsUrls.mockResolvedValue([])
})

afterEach(() => {
  vi.useRealTimers()
  vi.clearAllMocks()
  delete process.env.CROWD_DOCS_READINESS_SERP_API_KEY
  delete process.env.CROWD_DOCS_READINESS_VALIDATOR_ENABLED
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

  test('resolves a no-docs override to no URL, records it, and skips discovery', async () => {
    mocks.findActiveProjectDocOverride.mockResolvedValue({ docsUrl: null })

    const result = await resolveDocsUrl('project-1')

    expect(result).toEqual({
      docsUrl: null,
      discoveryMethod: 'override',
      confidence: 'authoritative',
      isOverride: true,
    })
    expect(mocks.upsertProjectDocDiscovery).toHaveBeenCalledWith(expect.anything(), {
      projectId: 'project-1',
      docsUrl: null,
      discoveryMethod: 'override',
      confidence: 'authoritative',
      candidates: [],
    })
    expect(mocks.findProjectForDocsDiscovery).not.toHaveBeenCalled()
    expect(mocks.discoverDocs).not.toHaveBeenCalled()
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
      docsValidator: null,
      deadlineAt: expect.any(Number),
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
      docsValidator: null,
      deadlineAt: expect.any(Number),
    })
  })

  describe('docs validator flag', () => {
    async function validatorPassedToDiscovery() {
      mocks.findActiveProjectDocOverride.mockResolvedValue(null)
      mocks.findProjectForDocsDiscovery.mockResolvedValue({
        id: 'project-1',
        slug: 'proj',
        name: 'Project',
        website: null,
        websiteSharedWith: [],
      })
      mocks.findEnabledRepositoriesForProject.mockResolvedValue([])
      mocks.discoverDocs.mockResolvedValue({
        docsUrl: null,
        discoveryMethod: null,
        confidence: null,
        allCandidates: [],
      })
      await resolveDocsUrl('project-1')
      return mocks.discoverDocs.mock.calls[0][0].docsValidator
    }

    test.each([undefined, 'false', '1', 'TRUE'])('is off when the env var is %s', async (value) => {
      if (value !== undefined) {
        process.env.CROWD_DOCS_READINESS_VALIDATOR_ENABLED = value
      }

      expect(await validatorPassedToDiscovery()).toBeNull()
      expect(mocks.createClient).not.toHaveBeenCalled()
    })

    test('is on only when the env var is exactly true', async () => {
      process.env.CROWD_DOCS_READINESS_VALIDATOR_ENABLED = 'true'

      expect(await validatorPassedToDiscovery()).toEqual(expect.any(Function))
      expect(mocks.createClient).toHaveBeenCalledTimes(1)
    })

    // The real validateDocsUrl turns every client failure into an `unclear` verdict; these run it
    // through the real pick logic to show a failed client keeps the stored URL.
    describe('with the real validateDocsUrl', () => {
      const README_PICK: IDocCandidate = {
        url: 'https://example.com/docs',
        method: 'readme-scrape',
        confidence: 'medium',
        livenessOk: true,
      }
      const REPO_PICK: IDocCandidate = {
        url: 'https://github.com/example/proj',
        method: 'repo-url',
        confidence: 'low',
        livenessOk: true,
      }
      const CREDENTIAL_VARS = [
        'CROWD_AKRITES_ANTHROPIC_AWS_REGION',
        'CROWD_AKRITES_ANTHROPIC_AWS_WORKSPACE_ID',
        'CROWD_AKRITES_ANTHROPIC_AWS_API_KEY',
      ]

      beforeEach(() => {
        process.env.CROWD_DOCS_READINESS_VALIDATOR_ENABLED = 'true'
        mocks.probe.mockResolvedValue({
          ok: true,
          status: 200,
          finalUrl: README_PICK.url,
          contentType: 'text/html',
        })
        mocks.fetchText.mockResolvedValue('<title>Some page</title>')
      })

      afterEach(() => {
        CREDENTIAL_VARS.forEach((name) => delete process.env[name])
        mocks.createClient.mockImplementation(() => ({ complete: vi.fn() }))
      })

      async function pick() {
        const docsValidator = await validatorPassedToDiscovery()
        const log = { info: vi.fn(), warn: vi.fn() }
        const winner = await pickValidatedWinner(
          {
            name: 'Project',
            slug: 'proj',
            website: null,
            websiteShared: false,
            repos: [],
            githubToken: null,
            serpApiKey: null,
            docsValidator,
            log,
          },
          [README_PICK, REPO_PICK],
          (pool) => pool[0] ?? null,
        )
        return { winner, log }
      }

      test('an HTTP error from the client keeps the pick', async () => {
        mocks.createClient.mockReturnValue({
          complete: vi.fn().mockRejectedValue(new Error('Anthropic API responded with HTTP 503')),
        })

        const { winner, log } = await pick()

        expect(winner).toBe(README_PICK)
        expect(log.warn).toHaveBeenCalledTimes(1)
        expect(log.warn.mock.calls[0][0]).toMatchObject({ outcome: 'validator-error' })
        expect(JSON.stringify(log.warn.mock.calls)).not.toContain('503')
      })

      test('missing credentials keep the pick', async () => {
        const actual = await vi.importActual<typeof import('../discovery/docsValidatorClient')>(
          '../discovery/docsValidatorClient',
        )
        mocks.createClient.mockImplementation(actual.createAnthropicAwsDocsValidatorClient)
        CREDENTIAL_VARS.forEach((name) => delete process.env[name])

        const { winner, log } = await pick()

        expect(winner).toBe(README_PICK)
        expect(log.warn.mock.calls[0][0]).toMatchObject({
          outcome: 'validator-error',
          errorName: 'Error',
        })
        expect(JSON.stringify(log.warn.mock.calls)).not.toContain('CROWD_AKRITES')
      })

      test('a client that never answers keeps the pick after the 20 s timeout', async () => {
        vi.useFakeTimers()
        mocks.createClient.mockReturnValue({ complete: vi.fn(() => new Promise<string>(() => {})) })

        const done = pick()
        await vi.advanceTimersByTimeAsync(20_001)
        const { winner, log } = await done

        expect(winner).toBe(README_PICK)
        expect(log.warn.mock.calls[0][0]).toMatchObject({ outcome: 'validator-error' })
      })

      test('a genuine unclear answer still drops the pick', async () => {
        mocks.createClient.mockReturnValue({
          complete: vi.fn().mockResolvedValue('{"verdict":"unclear","reason":"too little text"}'),
        })

        const { winner, log } = await pick()

        expect(winner).toBe(REPO_PICK)
        expect(log.warn).not.toHaveBeenCalled()
        expect(log.info.mock.calls[0][0]).toMatchObject({ verdict: 'unclear' })
      })

      test('a documents_project answer keeps the pick', async () => {
        mocks.createClient.mockReturnValue({
          complete: vi.fn().mockResolvedValue('{"verdict":"documents_project","reason":"ok"}'),
        })

        expect((await pick()).winner).toBe(README_PICK)
      })
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
