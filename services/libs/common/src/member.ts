import merge from 'lodash.merge'
import ldSum from 'lodash.sum'

import {
  MemberIdentityType,
  MemberOrganizationDateInput,
  MemberOrganizationDateRange,
  OrganizationSource,
} from '@crowd/types'

import { isValidEmail } from './email'

/* eslint-disable @typescript-eslint/no-explicit-any */

export function isSameMemberIdentity(
  a: { platform: string; type: string; value: string },
  b: { platform: string; type: string; value: string },
): boolean {
  return (
    a.platform === b.platform &&
    a.type === b.type &&
    a.value.trim().toLowerCase() === b.value.trim().toLowerCase()
  )
}

/** Email-shaped → lowercase; otherwise trim and keep preferred casing. */
export function normalizeMemberIdentityValue(value: string): string {
  const trimmed = value.trim()
  const lower = trimmed.toLowerCase()
  return isValidEmail(lower) ? lower : trimmed
}

/** Normalize values, drop empties, dedupe by (platform, type, lower(value)) preferring verified. */
export function normalizeMemberIdentities<
  T extends { platform: string; type: string; value: string; verified?: boolean },
>(identities: T[], options: { dropInvalidEmails?: boolean } = {}): T[] {
  const seen = new Map<string, T>()

  for (const identity of identities) {
    if (!identity.value?.trim()) {
      continue
    }

    const normalized = {
      ...identity,
      value: normalizeMemberIdentityValue(identity.value),
    }

    if (
      options.dropInvalidEmails &&
      normalized.type === MemberIdentityType.EMAIL &&
      !isValidEmail(normalized.value)
    ) {
      continue
    }

    const key = `${normalized.platform}:${normalized.type}:${normalized.value.toLowerCase()}`
    const existing = seen.get(key)
    if (!existing || (!existing.verified && normalized.verified)) {
      seen.set(key, normalized)
    }
  }

  return Array.from(seen.values())
}

/** Prefer `default`, else first non-empty string source value. */
export function getAttributeValue(
  attribute: Record<string, any> | null | undefined,
): string | undefined {
  if (!attribute) {
    return undefined
  }

  if (typeof attribute.default === 'string' && attribute.default.trim()) {
    return attribute.default
  }

  for (const [key, value] of Object.entries(attribute)) {
    if (key === 'default') continue
    if (typeof value === 'string' && value.trim()) {
      return value
    }
  }

  return undefined
}

export function hasAttributeValue(attribute: Record<string, any> | null | undefined): boolean {
  return Object.values(attribute || {}).some((v) => typeof v === 'string' && v.trim().length > 0)
}

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
  if (source === OrganizationSource.PROJECT_REGISTRY) return 1
  if (source === OrganizationSource.EMAIL_DOMAIN) return 2
  if (source?.startsWith('enrichment-')) return 3
  return 4
}

/**
 * Normalizes and validates a member's date range.
 * If throwError is true, it throws descriptive errors on failure.
 * Otherwise, it returns nulls for invalid ranges.
 */
export function sanitizeMemberOrganizationDateRange(
  dateStart: MemberOrganizationDateInput,
  dateEnd: MemberOrganizationDateInput,
  throwError = false,
): MemberOrganizationDateRange {
  const normalize = (date: MemberOrganizationDateInput) =>
    date === undefined || date === null || date === '' ? null : date

  const start = normalize(dateStart)
  const end = normalize(dateEnd)

  const handleError = (message: string): MemberOrganizationDateRange => {
    if (throwError) throw new Error(message)
    return { dateStart: null, dateEnd: null }
  }

  if (end && !start) {
    return handleError('Member organization with dateEnd and without dateStart!')
  }

  const startTime = start ? new Date(start).getTime() : null
  const endTime = end ? new Date(end).getTime() : null

  if ((start && Number.isNaN(startTime)) || (end && Number.isNaN(endTime))) {
    return handleError('Invalid member organization date format!')
  }

  if (startTime !== null && endTime !== null && endTime < startTime) {
    return handleError('Member organization with dateEnd before dateStart!')
  }

  return { dateStart: start, dateEnd: end }
}
