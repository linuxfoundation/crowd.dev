import { describe, expect, it } from 'vitest'

import { PlatformType } from '@crowd/types'

import { socialHandles, socialProfile } from './socials'

describe('socialProfile', () => {
  it.each([
    ['https://github.com/octocat', PlatformType.GITHUB, 'octocat'],
    ['github.com/octocat/hello-world', PlatformType.GITHUB, 'octocat'],
    ['https://www.linkedin.com/in/jane-doe/', PlatformType.LINKEDIN, 'jane-doe'],
    ['https://uk.linkedin.com/in/jane-doe', PlatformType.LINKEDIN, 'jane-doe'],
    ['https://twitter.com/jdoe', PlatformType.TWITTER, 'jdoe'],
    ['https://x.com/@jdoe', PlatformType.TWITTER, 'jdoe'],
  ])('reads %s', (link, platform, handle) => {
    expect(socialProfile(link)).toEqual({ platform, handle })
  })

  it.each([
    'https://github.com/orgs/acme',
    'https://gist.github.com/octocat',
    'https://www.linkedin.com/company/acme',
    'https://twitter.com/intent/tweet',
    'https://janedoe.dev',
    'not a url',
  ])('ignores %s', (link) => {
    expect(socialProfile(link)).toBeNull()
  })
})

describe('socialHandles', () => {
  it('takes the website first, then links in the bio, one handle per platform', () => {
    const about =
      'Maintainer. <a href="https://github.com/other">code</a>, linkedin.com/in/jane-doe and x.com/jdoe.'

    expect(socialHandles('https://github.com/octocat', about)).toEqual({
      [PlatformType.GITHUB]: 'octocat',
      [PlatformType.LINKEDIN]: 'jane-doe',
      [PlatformType.TWITTER]: 'jdoe',
    })
  })

  it('reads links written right after a label', () => {
    expect(socialHandles(undefined, 'GitHub:https://github.com/octocat; X:x.com/jdoe')).toEqual({
      [PlatformType.GITHUB]: 'octocat',
      [PlatformType.TWITTER]: 'jdoe',
    })
  })

  it('does not read a handle from inside another domain or URL path', () => {
    expect(socialHandles(undefined, 'Files on dropbox.com/team and netflix.com/browse')).toEqual({})
    expect(
      socialHandles(
        undefined,
        'See https://example.org/github.com/octocat and <a href="https://example.org/linkedin.com/in/jane-doe">me</a>',
      ),
    ).toEqual({})
  })

  it('returns nothing without a website or bio', () => {
    expect(socialHandles(undefined, undefined)).toEqual({})
  })
})
