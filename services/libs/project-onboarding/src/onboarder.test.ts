import { afterEach, describe, expect, it, vi } from 'vitest'

import { deriveProjectNameCandidates, readErrorBody, resolveProjectSegment } from './onboarder'

interface IFakeSegment {
  name: string
  id: string
  connectedRepoUrls?: string[]
}

interface IFakeBackend {
  segmentsByName: Map<string, string>
  connectedRepoUrlsBySegment: Map<string, string[]>
  createdNames: string[]
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body))
}

function installFakeBackend(existing: IFakeSegment[]): IFakeBackend {
  const backend: IFakeBackend = {
    segmentsByName: new Map(existing.map(({ name, id }) => [name.toLowerCase(), id])),
    connectedRepoUrlsBySegment: new Map(
      existing.flatMap(({ id, connectedRepoUrls }) =>
        connectedRepoUrls ? [[id, connectedRepoUrls] as [string, string[]]] : [],
      ),
    ),
    createdNames: [],
  }

  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init: { body: string }) => {
      const body = JSON.parse(init.body)

      if (url.endsWith('/segment/project/query')) {
        const id = backend.segmentsByName.get(String(body.filter.name).toLowerCase())
        const subprojects = id ? [{ id, name: body.filter.name }] : []
        return jsonResponse({ rows: [{ subprojects }], count: subprojects.length })
      }

      if (url.endsWith('/integration/query')) {
        const repoUrls = backend.connectedRepoUrlsBySegment.get(body.segments[0])
        const rows = repoUrls
          ? [{ settings: { orgs: [{ repos: repoUrls.map((url) => ({ url })) }] } }]
          : []
        return jsonResponse({ rows, count: rows.length })
      }

      backend.createdNames.push(body.name)
      backend.segmentsByName.set(body.name.toLowerCase(), `created-${body.name}`)
      return jsonResponse({})
    }),
  )

  return backend
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('deriveProjectNameCandidates', () => {
  it('returns the repo name first and the owner-qualified name second', () => {
    expect(deriveProjectNameCandidates('ai-dynamo', 'dynamo')).toEqual([
      'Dynamo',
      'Ai Dynamo Dynamo',
    ])
  })
})

describe('resolveProjectSegment', () => {
  const candidates = ['Dynamo', 'Ai Dynamo Dynamo']
  const repoUrl = 'https://github.com/ai-dynamo/dynamo'
  const otherRepoUrl = 'https://github.com/DynamoDS/Dynamo'

  it('creates a segment with the repo name when no project uses it', async () => {
    const backend = installFakeBackend([])

    const segmentId = await resolveProjectSegment(
      candidates,
      'slug',
      repoUrl,
      'http://api',
      'token',
    )

    expect(backend.createdNames).toEqual(['Dynamo'])
    expect(segmentId).toBe('created-Dynamo')
  })

  it('reuses an existing segment with the same name when it has no GitHub integration', async () => {
    const backend = installFakeBackend([{ name: 'Dynamo', id: 'empty-segment' }])

    const segmentId = await resolveProjectSegment(
      candidates,
      'slug',
      repoUrl,
      'http://api',
      'token',
    )

    expect(segmentId).toBe('empty-segment')
    expect(backend.createdNames).toEqual([])
  })

  it('creates an owner-qualified segment when the repo name is used by a connected project', async () => {
    const backend = installFakeBackend([
      { name: 'Dynamo', id: 'dynamods-segment', connectedRepoUrls: [otherRepoUrl.toLowerCase()] },
    ])

    const segmentId = await resolveProjectSegment(
      candidates,
      'slug',
      repoUrl,
      'http://api',
      'token',
    )

    expect(backend.createdNames).toEqual(['Ai Dynamo Dynamo'])
    expect(segmentId).toBe('created-Ai Dynamo Dynamo')
  })

  it('reuses the owner-qualified segment on a retry when it has no GitHub integration', async () => {
    const backend = installFakeBackend([
      { name: 'Dynamo', id: 'dynamods-segment', connectedRepoUrls: [otherRepoUrl.toLowerCase()] },
      { name: 'Ai Dynamo Dynamo', id: 'qualified-segment' },
    ])

    const segmentId = await resolveProjectSegment(
      candidates,
      'slug',
      repoUrl,
      'http://api',
      'token',
    )

    expect(segmentId).toBe('qualified-segment')
    expect(backend.createdNames).toEqual([])
  })

  it('reuses the segment already connected to the requested repo on a retry', async () => {
    const backend = installFakeBackend([
      { name: 'Dynamo', id: 'dynamods-segment', connectedRepoUrls: [otherRepoUrl.toLowerCase()] },
      { name: 'Ai Dynamo Dynamo', id: 'qualified-segment', connectedRepoUrls: [repoUrl] },
    ])

    const segmentId = await resolveProjectSegment(
      candidates,
      'slug',
      repoUrl,
      'http://api',
      'token',
    )

    expect(segmentId).toBe('qualified-segment')
    expect(backend.createdNames).toEqual([])
  })

  it('matches the requested repo url case-insensitively', async () => {
    installFakeBackend([
      {
        name: 'Dynamo',
        id: 'own-segment',
        connectedRepoUrls: ['https://github.com/ai-dynamo/dynamo'],
      },
    ])

    const segmentId = await resolveProjectSegment(
      candidates,
      'slug',
      'https://github.com/AI-Dynamo/Dynamo',
      'http://api',
      'token',
    )

    expect(segmentId).toBe('own-segment')
  })

  it('throws when every candidate name is used by a connected project', async () => {
    const backend = installFakeBackend([
      { name: 'Dynamo', id: 'dynamods-segment', connectedRepoUrls: [otherRepoUrl.toLowerCase()] },
      {
        name: 'Ai Dynamo Dynamo',
        id: 'qualified-segment',
        connectedRepoUrls: [otherRepoUrl.toLowerCase()],
      },
    ])

    await expect(
      resolveProjectSegment(candidates, 'slug', repoUrl, 'http://api', 'token'),
    ).rejects.toThrow(
      'Every candidate project name ("Dynamo", "Ai Dynamo Dynamo") is already used by a project with a GitHub integration',
    )
    expect(backend.createdNames).toEqual([])
  })

  it('throws when the integration query fails', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) =>
        url.endsWith('/segment/project/query')
          ? jsonResponse({ rows: [{ subprojects: [{ id: 'segment', name: 'Dynamo' }] }], count: 1 })
          : new Response('boom', { status: 500 }),
      ),
    )

    await expect(
      resolveProjectSegment(candidates, 'slug', repoUrl, 'http://api', 'token'),
    ).rejects.toThrow('Integration query returned HTTP 500')
  })
})

describe('readErrorBody', () => {
  it('returns the response body text', async () => {
    const response = new Response('{"error":"insightsProjects slug already exists"}')

    expect(await readErrorBody(response)).toBe('{"error":"insightsProjects slug already exists"}')
  })

  it('truncates a body longer than 500 characters', async () => {
    const response = new Response('a'.repeat(600))

    const body = await readErrorBody(response)

    expect(body).toBe(`${'a'.repeat(500)}…`)
  })

  it('returns an empty string when the body cannot be read', async () => {
    const response = new Response(null)
    // Consuming the body once locks the stream, so a second read fails.
    await response.text()

    expect(await readErrorBody(response)).toBe('')
  })
})
