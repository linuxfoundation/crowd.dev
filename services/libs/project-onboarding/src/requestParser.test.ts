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
  it('follows an organization page the model filed as a repository', async () => {
    const llm = llmAnswering(
      rawAnswer({ repoUrls: ['https://github.com/alacritty/'], projectName: 'Alacritty' }),
    )

    await expect(
      parseOnboardingRequest('onboard Alacritty, https://github.com/alacritty/', llm),
    ).resolves.toMatchObject({
      ok: true,
      request: {
        githubRepoUrls: [],
        linksToFollow: ['https://github.com/alacritty/'],
        projectName: 'Alacritty',
      },
    })
  })

  it('drops an organization page filed as a repository that is not in the request', async () => {
    const llm = llmAnswering(rawAnswer({ repoUrls: ['https://github.com/other/'] }))

    await expect(
      parseOnboardingRequest('onboard https://github.com/acme/', llm),
    ).resolves.toMatchObject({ ok: true, request: { githubRepoUrls: [], linksToFollow: [] } })
  })

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

  it('matches links the requester wrapped in Slack formatting', async () => {
    const text = 'onboard Acme *https://github.com/acme/* and `https://github.com/acme/one`'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/one'],
        linkUrls: ['https://github.com/acme/'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: {
        githubRepoUrls: ['https://github.com/acme/one'],
        linksToFollow: ['https://github.com/acme/'],
      },
    })
  })

  it('keeps a trailing underscore that is part of the repository name', async () => {
    const text = 'onboard https://github.com/acme/my_repo_ and acme/other_'
    const llm = llmAnswering(
      rawAnswer({
        repoUrls: ['https://github.com/acme/my_repo_', 'https://github.com/acme/other_'],
      }),
    )

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: {
        githubRepoUrls: ['https://github.com/acme/my_repo_', 'https://github.com/acme/other_'],
      },
    })
  })

  it('matches repositories in a request written as one italic phrase or as italic tokens', async () => {
    const llm = () =>
      llmAnswering(
        rawAnswer({ repoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] }),
      )
    const expected = {
      ok: true,
      request: { githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] },
    }

    await expect(
      parseOnboardingRequest('_onboard github.com/acme/one and github.com/acme/two_.', llm()),
    ).resolves.toMatchObject(expected)
    await expect(
      parseOnboardingRequest('_github.com/acme/one_ and _github.com/acme/two_', llm()),
    ).resolves.toMatchObject(expected)
    await expect(
      parseOnboardingRequest('_github.com/acme/one_, and _github.com/acme/two_', llm()),
    ).resolves.toMatchObject(expected)
  })

  it('caps the web pages to follow but keeps every GitHub organization link', async () => {
    const organizations = Array.from({ length: 6 }, (_, i) => `https://github.com/org-${i + 1}`)
    const pages = Array.from({ length: 7 }, (_, i) => `https://acme.org/page-${i + 1}`)
    const text = `onboard ${[...pages, ...organizations].join(' ')}`
    const llm = llmAnswering(rawAnswer({ linkUrls: [...pages, ...organizations] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { linksToFollow: [...organizations, ...pages.slice(0, 5)] },
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

  it('returns the link exactly as written in the request, not the model spelling', async () => {
    const text = 'The list is at https://www.example.com/Project'
    const llm = llmAnswering(rawAnswer({ linkUrls: ['http://example.com/Project/'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({
      ok: true,
      request: { linksToFollow: ['https://www.example.com/Project'] },
    })
  })

  it('does not treat links that differ only by path case as the same link', async () => {
    const text = 'The list is at https://example.com/Project'
    const llm = llmAnswering(rawAnswer({ linkUrls: ['https://example.com/project'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { linksToFollow: [] } })
  })

  it('does not treat a link with a .git suffix as the link without it', async () => {
    const text = 'The list is at https://example.com/archive'
    const llm = llmAnswering(rawAnswer({ linkUrls: ['https://example.com/archive.git'] }))

    const result = await parseOnboardingRequest(text, llm)

    expect(result).toMatchObject({ ok: true, request: { linksToFollow: [] } })
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

    expect(result).toEqual({
      ok: false,
      reason: expect.stringContaining('boom'),
      retryable: true,
    })
  })

  it('fails when the LLM returns nothing', async () => {
    const llm = vi.fn().mockResolvedValue(undefined)

    const result = await parseOnboardingRequest('onboard acme/one', llm)

    expect(result).toEqual({
      ok: false,
      reason: 'LLM query returned no response',
      retryable: true,
    })
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

  it('rejects oversized requests without querying the LLM', async () => {
    const llm = llmAnswering(rawAnswer())

    const result = await parseOnboardingRequest('a'.repeat(50_000), llm)

    expect(result).toEqual({
      ok: false,
      reason: 'Request text is too long (50000 characters, max 10000)',
    })
    expect(llm).not.toHaveBeenCalled()
  })

  it('accepts a request exactly at the length limit', async () => {
    const llm = llmAnswering(rawAnswer())

    const result = await parseOnboardingRequest('a'.repeat(10_000), llm)

    expect(result.ok).toBe(true)
  })
})

describe('buildOnboardingRequestPrompt', () => {
  it('wraps the request in tags as untrusted content', () => {
    const prompt = buildOnboardingRequestPrompt('hello')

    expect(prompt).toContain('<request>\nhello\n</request>')
  })
})
