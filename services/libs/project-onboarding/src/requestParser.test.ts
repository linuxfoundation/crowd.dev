import { describe, expect, it, vi } from 'vitest'

import { buildOnboardingRequestPrompt, parseOnboardingRequest } from './requestParser'

function llmAnswering(payload: unknown) {
  return vi.fn().mockResolvedValue(typeof payload === 'string' ? payload : JSON.stringify(payload))
}

function rawAnswer(overrides: Record<string, unknown> = {}) {
  return {
    repoUrls: [],
    linkUrls: [],
    projectName: null,
    declaredLf: null,
    asksAboutHierarchy: false,
    ...overrides,
  }
}

describe('parseOnboardingRequest', () => {
  it('extracts and canonicalizes multiple GitHub repositories', async () => {
    const text = 'Please onboard https://github.com/acme/one and acme/two for project Acme'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two.git'],
        projectName: ' Acme ',
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toEqual({
      ok: true,
      request: expect.objectContaining({
        githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'],
        projectName: 'Acme',
      }),
    })
  })

  it('separates non-GitHub repositories', async () => {
    const text = 'Our code lives at https://gitlab.com/acme/tool'
    const llm = llmAnswering(rawAnswer({ repoUrls: ['https://gitlab.com/acme/tool'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { githubRepoUrls: [], nonGithubRepoUrls: ['https://gitlab.com/acme/tool'] },
    })
  })

  it('keeps links to follow and drops those that are requested repositories', async () => {
    const text = 'Repos are listed at https://acme.org/projects and https://github.com/acme/one'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/one'],
        linkUrls: ['https://acme.org/projects', 'https://github.com/acme/one'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { linksToFollow: ['https://acme.org/projects'] },
    })
  })

  it('reports hierarchy questions and the declared LF flag', async () => {
    const llm = llmAnswering(rawAnswer({ declaredLf: true, asksAboutHierarchy: true }))

    const result = await parseOnboardingRequest('We are an LF project, where do we go?', llm)

    expect(result).toMatchObject({
      ok: true,
      request: { declaredLf: true, asksAboutHierarchy: true },
    })
  })

  it('drops repositories and links the LLM hallucinated', async () => {
    const text = 'Please onboard https://github.com/acme/real'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/real', 'https://github.com/evil/invented'],
        linkUrls: ['https://made-up.example/list'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { githubRepoUrls: ['https://github.com/acme/real'], linksToFollow: [] },
    })
  })

  it('does not accept a repository that is only a prefix of one in the request', async () => {
    const text = 'Please onboard https://github.com/acme/toolkit'
    const llm = llmAnswering(rawAnswer({ repoUrls: ['https://github.com/acme/tool'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { githubRepoUrls: [] } })
  })

  it('drops an invented path on a host that appears in the request', async () => {
    const text = 'Please onboard https://github.com/acme/real and https://gitlab.com/acme/tool'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://gitlab.com/acme/invented'],
        linkUrls: ['https://github.com/acme/real/wiki/invented'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { nonGithubRepoUrls: [], linksToFollow: [] },
    })
  })

  it('excludes a requested repository emitted as a link in another form', async () => {
    const text = 'Please onboard https://github.com/acme/one'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/one'],
        linkUrls: ['http://github.com/Acme/one.git', 'https://github.com/acme/one/'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { linksToFollow: [] } })
  })

  it('requires the repository host to match the request', async () => {
    const text = 'Please onboard https://gitlab.com/acme/tool'
    const llm = llmAnswering(rawAnswer({ repoUrls: ['https://github.com/acme/tool'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { githubRepoUrls: [] } })
  })

  it('accepts scp-style and ssh clone URLs for non-GitHub hosts', async () => {
    const text = 'clone git@gitlab.com:acme/tool.git or ssh://git@bitbucket.org/acme/other.git'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://gitlab.com/acme/tool', 'https://bitbucket.org/acme/other'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: {
        nonGithubRepoUrls: ['https://gitlab.com/acme/tool', 'https://bitbucket.org/acme/other'],
      },
    })
  })

  it('does not accept a prefix of a dotted repository name', async () => {
    const text = 'Please onboard vercel/next.js'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/vercel/next', 'https://github.com/vercel/next.js'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { githubRepoUrls: ['https://github.com/vercel/next.js'] },
    })
  })

  it('drops a link whose query string was not in the request', async () => {
    const text = 'Repos are listed at https://acme.org/projects'
    const llm = llmAnswering(rawAnswer({ linkUrls: ['https://acme.org/projects?target=evil'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { linksToFollow: [] } })
  })

  it('keeps a deep link of a repository that is also being onboarded', async () => {
    const text =
      'Onboard https://github.com/acme/one, the list is in https://github.com/acme/one/wiki/Repos'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/one'],
        linkUrls: ['https://github.com/acme/one/wiki/Repos'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { linksToFollow: ['https://github.com/acme/one/wiki/Repos'] },
    })
  })

  it('ignores non-web link protocols', async () => {
    const text = 'see ftp://acme.org/list'
    const llm = llmAnswering(rawAnswer({ linkUrls: ['ftp://acme.org/list'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { linksToFollow: [] } })
  })

  it('accepts JSON wrapped in a markdown fence', async () => {
    const text = 'onboard acme/one'
    const llm = llmAnswering(
      '```json\n' +
        JSON.stringify(rawAnswer({ repoUrls: ['https://github.com/acme/one'] })) +
        '\n```',
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { githubRepoUrls: ['https://github.com/acme/one'] },
    })
  })

  it('fails on empty text without calling the LLM', async () => {
    const llm = vi.fn()

    const result = await parseOnboardingRequest('   ', llm)

    expect(result.ok).toBe(false)
    expect(llm).not.toHaveBeenCalled()
  })

  it('fails when the LLM throws', async () => {
    const llm = vi.fn().mockRejectedValue(new Error('boom'))

    const result = await parseOnboardingRequest('onboard acme/one', llm)

    expect(result).toEqual({ ok: false, reason: expect.stringContaining('boom') })
  })

  it('fails when the LLM returns nothing', async () => {
    const llm = vi.fn().mockResolvedValue(undefined)

    const result = await parseOnboardingRequest('onboard acme/one', llm)

    expect(result.ok).toBe(false)
  })

  it('fails on invalid JSON', async () => {
    const result = await parseOnboardingRequest('onboard acme/one', llmAnswering('not json'))

    expect(result.ok).toBe(false)
  })

  it('fails on a response with the wrong shape', async () => {
    const result = await parseOnboardingRequest(
      'onboard acme/one',
      llmAnswering({ repoUrls: 'https://github.com/acme/one' }),
    )

    expect(result).toEqual({ ok: false, reason: expect.stringContaining('shape') })
  })

  it('truncates very long requests before building the prompt', async () => {
    const llm = llmAnswering(rawAnswer())

    await parseOnboardingRequest('a'.repeat(50_000), llm)

    expect(llm.mock.calls[0][0].length).toBeLessThan(15_000)
  })
})

describe('buildOnboardingRequestPrompt', () => {
  it('wraps the request in tags as untrusted content', () => {
    const prompt = buildOnboardingRequestPrompt('hello')

    expect(prompt).toContain('<request>\nhello\n</request>')
  })
})
