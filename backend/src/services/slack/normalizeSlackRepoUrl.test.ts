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

  it('keeps underscores inside the url', () => {
    expect(normalizeSlackRepoUrl('`https://github.com/foo/bar_baz`')).toBe(
      'https://github.com/foo/bar_baz',
    )
  })
})
