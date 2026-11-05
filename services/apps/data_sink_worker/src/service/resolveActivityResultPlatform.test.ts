import { describe, expect, it } from 'vitest'

import { PlatformType } from '@crowd/types'

import {
  resolveActivityResultPlatform,
  skipsRepositorySegmentLookup,
} from './resolveActivityResultPlatform'

describe('resolveActivityResultPlatform', () => {
  it('prefers the activity platform over the integration platform when both are set', () => {
    expect(resolveActivityResultPlatform(PlatformType.OCTOLENS, PlatformType.REDDIT)).toBe(
      PlatformType.REDDIT,
    )
  })

  it('falls back to the integration platform when the activity has none', () => {
    expect(resolveActivityResultPlatform(PlatformType.GITHUB, undefined)).toBe(PlatformType.GITHUB)
  })

  it('normalizes github-nango to github after resolving', () => {
    expect(resolveActivityResultPlatform(PlatformType.GITHUB_NANGO, undefined)).toBe(
      PlatformType.GITHUB,
    )
  })

  it('normalizes an activity-provided github-nango to github too', () => {
    expect(resolveActivityResultPlatform(null, PlatformType.GITHUB_NANGO)).toBe(PlatformType.GITHUB)
  })

  it('returns null when neither the integration nor the activity has a platform', () => {
    expect(resolveActivityResultPlatform(null, undefined)).toBeNull()
  })
})

describe('skipsRepositorySegmentLookup', () => {
  it('skips the repositories lookup for activities from an Octolens integration', () => {
    expect(skipsRepositorySegmentLookup(PlatformType.OCTOLENS)).toBe(true)
  })

  it.each([PlatformType.GITHUB, PlatformType.GITHUB_NANGO, PlatformType.GITLAB, null])(
    'keeps the lookup for %s integrations',
    (platform) => {
      expect(skipsRepositorySegmentLookup(platform)).toBe(false)
    },
  )
})
