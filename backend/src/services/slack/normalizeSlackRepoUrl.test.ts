import { describe, expect, it } from 'vitest'

import { normalizeSlackRepoUrl } from './normalizeSlackRepoUrl'

describe('normalizeSlackRepoUrl', () => {
  const url = 'https://github.com/foo/bar'

  it.each([
    ['a plain url', url],
    ['backticks', `\`${url}\``],
    ['double backticks', `\`\`${url}\`\``],
    ['bold', `*${url}*`],
    ['italic', `_${url}_`],
    ['strikethrough', `~${url}~`],
    ['a slack link', `<${url}>`],
    ['a slack link with a label', `<${url}|foo/bar>`],
    ['a formatted slack link', `*<${url}|foo/bar>*`],
    ['surrounding whitespace', `  ${url}  `],
  ])('unwraps %s', (_label, raw) => {
    expect(normalizeSlackRepoUrl(raw)).toBe(url)
  })

  it.each([
    ['an underscore inside the path', 'https://github.com/foo/bar_baz'],
    ['a trailing underscore in the repo name', 'https://github.com/foo/bar_'],
    ['a leading underscore in the owner', 'https://github.com/_foo/bar'],
  ])('keeps %s', (_label, raw) => {
    expect(normalizeSlackRepoUrl(raw)).toBe(raw)
    expect(normalizeSlackRepoUrl(`\`${raw}\``)).toBe(raw)
  })

  it('returns a slack link target unchanged', () => {
    expect(normalizeSlackRepoUrl('<https://github.com/foo/bar_|foo/bar_>')).toBe(
      'https://github.com/foo/bar_',
    )
  })
})
