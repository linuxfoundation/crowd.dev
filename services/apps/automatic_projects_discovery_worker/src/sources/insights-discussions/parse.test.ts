import { describe, expect, it } from 'vitest'

import { extractDiscussionRepoUrls, extractRepoUrls, stripNonProseSections } from './parse'

describe('extractRepoUrls', () => {
  it('extracts a plain https URL', () => {
    expect(extractRepoUrls('See https://github.com/foo/bar for details')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a plain http URL', () => {
    expect(extractRepoUrls('http://github.com/foo/bar')).toEqual(['https://github.com/foo/bar'])
  })

  it('extracts a www. URL', () => {
    expect(extractRepoUrls('https://www.github.com/foo/bar')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a URL without a scheme', () => {
    expect(extractRepoUrls('please onboard github.com/agentnameservice/ans')).toEqual([
      'https://github.com/agentnameservice/ans',
    ])
  })

  it('strips a .git suffix', () => {
    expect(extractRepoUrls('https://github.com/foo/bar.git')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('strips trailing sentence punctuation', () => {
    expect(extractRepoUrls('Please onboard https://github.com/foo/bar.')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a URL from a markdown link', () => {
    expect(extractRepoUrls('[our repo](https://github.com/foo/bar)')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it.each([
    ['https://github.com/foo/bar/tree/main/src', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/blob/main/README.md', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/pull/42', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/issues', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/discussions', 'https://github.com/foo/bar'],
  ])('collapses deep link %s to the repo root', (input, expected) => {
    expect(extractRepoUrls(input)).toEqual([expected])
  })

  it('rejects reserved GitHub owners', () => {
    expect(extractRepoUrls('https://github.com/orgs/foo')).toEqual([])
    expect(extractRepoUrls('https://github.com/marketplace/foo')).toEqual([])
  })

  it('rejects an owner-only URL', () => {
    expect(extractRepoUrls('https://github.com/gardenlinux')).toEqual([])
  })

  it('folds case', () => {
    expect(extractRepoUrls('https://github.com/Foo/BAR')).toEqual(['https://github.com/foo/bar'])
  })

  it('dedupes repeated references, preserving first-seen order', () => {
    expect(
      extractRepoUrls(
        'https://github.com/foo/bar and again https://github.com/foo/bar, also https://github.com/baz/qux',
      ),
    ).toEqual(['https://github.com/foo/bar', 'https://github.com/baz/qux'])
  })
})

describe('stripNonProseSections', () => {
  it('removes fenced code blocks', () => {
    expect(
      stripNonProseSections('before\n```\nhttps://github.com/foo/bar\n```\nafter'),
    ).not.toContain('github.com/foo/bar')
  })

  it('removes inline code', () => {
    expect(stripNonProseSections('see `https://github.com/foo/bar` here')).not.toContain(
      'github.com/foo/bar',
    )
  })

  it('removes HTML comments', () => {
    expect(stripNonProseSections('<!-- https://github.com/foo/bar --> visible text')).not.toContain(
      'github.com/foo/bar',
    )
  })

  it('removes blockquote lines', () => {
    expect(stripNonProseSections('> https://github.com/foo/bar\nreal text')).not.toContain(
      'github.com/foo/bar',
    )
  })

  it('keeps prose text intact', () => {
    expect(stripNonProseSections('please onboard https://github.com/foo/bar')).toContain(
      'github.com/foo/bar',
    )
  })
})

describe('extractDiscussionRepoUrls', () => {
  it('extracts a reference that only appears in the title', () => {
    const result = extractDiscussionRepoUrls({
      title: 'github.com/agentnameservice/ans',
      body: 'Please onboard this project, thanks!',
    })

    expect(result.repoUrls).toEqual(['https://github.com/agentnameservice/ans'])
    expect(result.fromTitle).toBe(1)
    expect(result.fromBody).toBe(0)
  })

  it('scans the title before the body and dedupes across both', () => {
    const result = extractDiscussionRepoUrls({
      title: 'Onboard https://github.com/foo/bar',
      body: 'Same repo again: https://github.com/foo/bar',
    })

    expect(result.repoUrls).toEqual(['https://github.com/foo/bar'])
    expect(result.fromTitle).toBe(1)
    expect(result.fromBody).toBe(1)
  })

  it('strips quoted/code references from the body but keeps prose ones', () => {
    const result = extractDiscussionRepoUrls({
      title: 'Onboard request',
      body: '> quoted mention of https://github.com/quoted/repo\nPlease onboard https://github.com/real/repo',
    })

    expect(result.repoUrls).toEqual(['https://github.com/real/repo'])
  })

  it('keeps all six references when several repos of the same discussion survive', () => {
    const repos = [
      'cockpit-project/cockpit',
      'cockpit-project/cockpit-machines',
      'cockpit-project/cockpit-podman',
      'cockpit-project/cockpit-ostree',
      'cockpit-project/cockpit-files',
      'cockpit-project/cockpit-navigator',
    ]
    const body = repos.map((r) => `https://github.com/${r}`).join('\n')

    const result = extractDiscussionRepoUrls({ title: 'Onboard the Cockpit project', body })

    expect(result.repoUrls).toEqual(repos.map((r) => `https://github.com/${r}`))
  })

  it('returns no candidates for a discussion with no repo references', () => {
    const result = extractDiscussionRepoUrls({
      title: 'Garden Linux: Organization vs Project',
      body: 'https://github.com/gardenlinux',
    })

    expect(result.repoUrls).toEqual([])
  })
})
