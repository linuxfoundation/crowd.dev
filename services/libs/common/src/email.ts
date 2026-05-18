import validator from 'validator'

import { noreplyEmailProviders } from './constants'

export const isValidEmail = (value: string): boolean => {
  return validator.isEmail(value)
}

/**
 * Extracts username from a GitHub noreply email.
 * @see https://docs.github.com/en/account-and-profile/reference/email-addresses-reference#your-noreply-email-address
 */
export const parseGitHubNoreplyEmail = (email?: string | null): string | null => {
  if (!email) return null

  const lower = email.toLowerCase()
  if (!lower.endsWith(noreplyEmailProviders.github)) return null

  const local = lower.slice(0, -noreplyEmailProviders.github.length)
  if (!local) return null

  const plusIndex = local.indexOf('+')
  const username = plusIndex >= 0 ? local.slice(plusIndex + 1) : local

  return username || null
}

/**
 * Extracts username from a GitLab noreply email.
 * @see https://docs.gitlab.com/user/profile/#use-an-automatically-generated-private-commit-email
 */
export const parseGitLabNoreplyEmail = (email?: string | null): string | null => {
  if (!email) return null

  const lower = email.toLowerCase()
  if (!lower.endsWith(noreplyEmailProviders.gitlab)) return null

  const local = lower.slice(0, -noreplyEmailProviders.gitlab.length)
  if (!local) return null

  // Strip numeric ID prefix (e.g. "12345-username" → "username")
  const username = /^\d+-/.test(local) ? local.slice(local.indexOf('-') + 1) : local
  return username || null
}

/**
 * Returns true if the email host indicates a local machine (not a real mail server).
 * Git commits from unconfigured machines often produce addresses like user@hostname.local.
 */
export const isLocalMachineEmail = (value: string): boolean => {
  const atIndex = value.indexOf('@')
  if (atIndex < 0) return false
  const host = value.slice(atIndex + 1).toLowerCase()
  return (
    host === 'localhost' ||
    host.endsWith('.local') ||
    host.endsWith('.lan') ||
    host.endsWith('.localdomain')
  )
}

/**
 * Returns the local-part of an email (before '@'), lower-cased.
 * Returns the full value lower-cased when there is no '@'.
 */
export const getEmailLocalPart = (value: string): string => {
  const atIndex = value.indexOf('@')
  return (atIndex >= 0 ? value.slice(0, atIndex) : value).toLowerCase()
}
