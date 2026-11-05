import { PlatformType } from '@crowd/types'

// Octolens emits activities across many real platforms from one integration, so the
// activity's own platform must win — every other integration today only ever has one.
export function resolveActivityResultPlatform(
  integrationPlatform: PlatformType | null,
  activityPlatform: PlatformType | undefined,
): PlatformType | null {
  const platform = activityPlatform ?? integrationPlatform
  return platform === PlatformType.GITHUB_NANGO ? PlatformType.GITHUB : platform
}

// Octolens routes each mention to its keyword's project, never through public.repositories.
export function skipsRepositorySegmentLookup(integrationPlatform: PlatformType | null): boolean {
  return integrationPlatform === PlatformType.OCTOLENS
}
