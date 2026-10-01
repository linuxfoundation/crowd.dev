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
