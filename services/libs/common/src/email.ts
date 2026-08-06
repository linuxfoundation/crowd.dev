import validator from 'validator'

import { disposableEmailDomains, emailSafeKnownBots, noreplyEmailProviders } from './constants'

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

export type EmailReachabilityReason =
  | 'invalid'
  | 'noreply'
  | 'bot'
  | 'local-machine'
  | 'disposable'
  | 'placeholder-domain'

export interface EmailReachability {
  email: string
  reachable: boolean
  reason: EmailReachabilityReason | null
}

const PLACEHOLDER_EMAIL_DOMAINS = new Set([
  'example.com',
  'example.org',
  'example.net',
  'example.edu',
])

const NOREPLY_LOCAL_PARTS = new Set(['noreply', 'no-reply', 'donotreply', 'do-not-reply'])

// Role mailboxes are prime security contacts — never classify them as bots.
const ROLE_LOCAL_PARTS = new Set([
  'security',
  'psirt',
  'abuse',
  'support',
  'admin',
  'info',
  'contact',
])

const BOT_EXACT_LOCAL_PARTS = new Set([
  'dependabot',
  'renovate',
  'ci',
  'jenkins',
  'github-actions',
  'actions',
])

const emailDomain = (value: string): string => value.slice(value.indexOf('@') + 1)

const looksLikeBot = (localPart: string): boolean => {
  const lp = localPart.toLowerCase()
  if (ROLE_LOCAL_PARTS.has(lp)) return false
  if (emailSafeKnownBots.has(lp)) return true
  if (lp.endsWith('[bot]')) return true
  if (BOT_EXACT_LOCAL_PARTS.has(lp)) return true
  // 'bot' as a delimited token: bot-*, *-bot, *.bot, bot_* ...
  return /(^|[.\-_+])bot([.\-_+]|$)/.test(lp)
}

/**
 * Heuristic reachability classification for a single email. Pure and synchronous.
 * MX/SMTP validation is intentionally out of scope.
 */
export const classifyEmailReachability = (email: string): EmailReachability => {
  const value = email.trim().toLowerCase()
  const fail = (reason: EmailReachabilityReason): EmailReachability => ({
    email,
    reachable: false,
    reason,
  })

  // Checked before isValidEmail: github/gitlab noreply addresses (a bracketed `[bot]`
  // local-part) and local-machine hosts (no TLD) both fail RFC validation, but their
  // specific reason is more useful than a blanket 'invalid'.
  if (parseGitHubNoreplyEmail(value) != null || parseGitLabNoreplyEmail(value) != null) {
    return fail('noreply')
  }
  if (isLocalMachineEmail(value)) return fail('local-machine')

  if (!isValidEmail(value)) return fail('invalid')
  if (PLACEHOLDER_EMAIL_DOMAINS.has(emailDomain(value))) return fail('placeholder-domain')

  const localPart = getEmailLocalPart(value)
  const baseLocalPart = localPart.split('+')[0]
  if (NOREPLY_LOCAL_PARTS.has(baseLocalPart)) return fail('noreply')
  if (looksLikeBot(baseLocalPart)) return fail('bot')
  if (disposableEmailDomains.has(emailDomain(value))) return fail('disposable')

  return { email, reachable: true, reason: null }
}

export const getReachableEmails = (emails: string[]): string[] =>
  emails.filter((e) => classifyEmailReachability(e).reachable)
