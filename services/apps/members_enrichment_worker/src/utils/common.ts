import lodash from 'lodash'

import { IMemberEnrichmentCache, MemberIdentityType, PlatformType } from '@crowd/types'

import {
  IMemberEnrichmentAttributeSettings,
  IMemberEnrichmentData,
  IMemberEnrichmentDataNormalized,
  IMemberEnrichmentSocialData,
} from '../types'

export function sourceHasDifferentDataComparedToCache(
  cache: IMemberEnrichmentCache<IMemberEnrichmentData>,
  data: IMemberEnrichmentData,
): boolean {
  return !cache || !lodash.isEqual(data, cache.data)
}

export function normalizeSocialIdentity(
  data: IMemberEnrichmentSocialData,
  identityType: MemberIdentityType,
  normalized: IMemberEnrichmentDataNormalized,
): IMemberEnrichmentDataNormalized {
  if (!normalized.identities) {
    normalized.identities = []
  }

  if (!normalized.attributes) {
    normalized.attributes = {}
  }

  normalized.identities.push({
    value: data.handle,
    type: identityType,
    platform: data.platform,
    verified: false,
    source: 'enrichment',
  })

  return normalized
}

export function getSocialUrl(data: IMemberEnrichmentSocialData): string | null {
  switch (data.platform) {
    case PlatformType.GITHUB:
      return `https://github.com/${data.handle}`
    case PlatformType.LINKEDIN:
      return `https://linkedin.com/in/${data.handle}`
    case PlatformType.TWITTER:
      return `https://twitter.com/${data.handle}`
    default:
      return null
  }
}

export function normalizeAttributes(
  data: IMemberEnrichmentData,
  normalized: IMemberEnrichmentDataNormalized,
  attributeSettings: IMemberEnrichmentAttributeSettings,
  platform: string,
): IMemberEnrichmentDataNormalized {
  if (!normalized.attributes) {
    normalized.attributes = {}
  }

  for (const attributeName in attributeSettings) {
    const attribute = attributeSettings[attributeName]

    let value = null

    for (const field of attribute.fields) {
      if (value) {
        break
      }
      // Get value from 'enriched' object using the defined mapping and 'lodash.get'
      value = lodash.get(data, field)
    }

    if ((Array.isArray(value) && value.length > 0) || (!Array.isArray(value) && value)) {
      // Check if 'member.attributes[attributeName]' exists, and if it does not, initialize it as an empty object
      if (!normalized.attributes[attributeName]) {
        normalized.attributes[attributeName] = {}
      }

      const fn = attribute.transform || ((value) => value)
      value = fn(value)

      normalized.attributes[attributeName][platform] = value
    }
  }

  return normalized
}

export function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}
