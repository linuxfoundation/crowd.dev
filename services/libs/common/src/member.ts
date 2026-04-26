import merge from 'lodash.merge'
import ldSum from 'lodash.sum'

import { OrganizationSource } from '@crowd/types'

/* eslint-disable @typescript-eslint/no-explicit-any */

export async function setAttributesDefaultValues(
  attributes: Record<string, unknown>,
  priorities: string[],
): Promise<Record<string, unknown>> {
  if (!priorities) {
    throw new Error(`No priorities set!`)
  }

  for (const attributeName of Object.keys(attributes)) {
    if (typeof attributes[attributeName] === 'string') {
      // we try to fix it
      attributes[attributeName] = JSON.parse(attributes[attributeName] as string)
    }

    const nonEmptyPlatform = Object.keys(attributes[attributeName]).filter((p) => {
      if (p === 'default') return false
      const value = attributes[attributeName][p]
      return value !== undefined && value !== null && String(value).trim().length > 0
    })

    const highestPriorityPlatform = getHighestPriorityPlatformForAttributes(
      nonEmptyPlatform,
      priorities,
    )

    if (highestPriorityPlatform !== undefined) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ;(attributes[attributeName] as any).default =
        attributes[attributeName][highestPriorityPlatform]
    } else {
      // Only delete if there is no existing non-empty default value.
      // An attribute with only a `default` key and no platform-specific keys
      // has no source platform to derive from, but its value should be preserved.
      const existingDefault = (attributes[attributeName] as any).default
      if (
        existingDefault === undefined ||
        existingDefault === null ||
        String(existingDefault).trim().length === 0
      ) {
        delete attributes[attributeName]
      }
    }
  }

  return attributes
}

export function getHighestPriorityPlatformForAttributes(
  platforms: string[],
  priorityArray: string[],
): string | undefined {
  if (platforms.length <= 0) {
    return undefined
  }
  const filteredPlatforms = priorityArray.filter((i) => platforms.includes(i))
  return filteredPlatforms.length > 0 ? filteredPlatforms[0] : platforms[0]
}

/**
 *
 * @param oldReach The old reach object
 * @param newReach the new reach object
 * @returns The new reach object
 */
export const calculateReach = (oldReach: any, newReach: any): { total: number } => {
  // Totals are recomputed, so we delete them first
  delete oldReach.total
  delete newReach.total
  const out = merge(oldReach, newReach)
  if (Object.keys(out).length === 0) {
    return { total: -1 }
  }
  // Total is the sum of all attributes
  out.total = ldSum(Object.values(out))
  return out
}

/**
 * Lower rank wins when multiple member-organization sources overlap.
 */
export function getMemberOrganizationSourceRank(source: string | null | undefined): number {
  if (source === OrganizationSource.UI) return 0
  if (source === OrganizationSource.EMAIL_DOMAIN) return 1
  if (source?.startsWith('enrichment-')) return 2
  return 3
}
